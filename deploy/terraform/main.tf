# A three-node jobq cluster on EC2, one node per availability zone.
#
# What this builds, and what it deliberately does not:
#
#   - A VPC with three public subnets. No NAT gateway: a NAT gateway bills
#     by the hour whether or not anything uses it, and these nodes only
#     need outbound HTTPS to reach S3, Systems Manager and CloudWatch.
#   - Three Graviton instances with fixed private addresses, so every
#     node's -peers list is known before any of them exists.
#   - No SSH. Shell access is through Systems Manager Session Manager,
#     which needs no inbound port and no key pair.
#   - The peer port (9090) is open only between the nodes themselves. It
#     has no authentication: whoever can reach it can speak Raft.
#   - The client port (8080) is open to the nodes and to var.client_cidrs,
#     which is empty by default. It has no authentication either.
#
# Read RUNBOOK.md before applying. Apply deploy/terraform/budget first.

locals {
  name = var.cluster_name

  # Node ID -> its place in the cluster. The private address is the tenth
  # host of each subnet.
  nodes = {
    for i in range(3) : tostring(i + 1) => {
      az         = data.aws_availability_zones.available.names[i]
      cidr       = cidrsubnet(var.vpc_cidr, 8, i + 1)
      private_ip = cidrhost(cidrsubnet(var.vpc_cidr, 8, i + 1), 10)
    }
  }

  peers     = join(",", [for id, n in local.nodes : "${id}=http://${n.private_ip}:9090"])
  advertise = join(",", [for id, n in local.nodes : "${id}=http://${n.private_ip}:8080"])

  binary_key = "jobqd-${filesha256(var.jobqd_binary)}"
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

data "aws_region" "current" {}

# --- network -----------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = local.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = local.name }
}

resource "aws_subnet" "node" {
  for_each                = local.nodes
  vpc_id                  = aws_vpc.this.id
  availability_zone       = each.value.az
  cidr_block              = each.value.cidr
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name}-${each.key}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }
  tags = { Name = local.name }
}

resource "aws_route_table_association" "node" {
  for_each       = aws_subnet.node
  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "node" {
  name        = "${local.name}-node"
  description = "jobq nodes: Raft between themselves, clients from an allow list"
  vpc_id      = aws_vpc.this.id
  tags        = { Name = "${local.name}-node" }
}

resource "aws_vpc_security_group_ingress_rule" "peer" {
  security_group_id            = aws_security_group.node.id
  description                  = "Raft, node to node only"
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = 9090
  to_port                      = 9090
}

resource "aws_vpc_security_group_ingress_rule" "client_from_nodes" {
  security_group_id            = aws_security_group.node.id
  description                  = "client API from the other nodes"
  referenced_security_group_id = aws_security_group.node.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}

resource "aws_vpc_security_group_ingress_rule" "client" {
  for_each          = toset(var.client_cidrs)
  security_group_id = aws_security_group.node.id
  description       = "client API from an allowed network"
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 8080
  to_port           = 8080
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.node.id
  description       = "outbound: S3, Systems Manager, CloudWatch"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- the binary --------------------------------------------------------------

# The nodes fetch jobqd from here at first boot. Built locally and uploaded,
# so a deployment does not depend on the instance being able to compile Go
# or on the repository being reachable.
resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${local.name}-artifacts-"
  force_destroy = true # so `terraform destroy` leaves nothing behind to bill
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "jobqd" {
  bucket      = aws_s3_bucket.artifacts.id
  key         = local.binary_key
  source      = var.jobqd_binary
  source_hash = filesha256(var.jobqd_binary)
}

# --- what a node is allowed to do ---------------------------------------------

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "node" {
  statement {
    sid       = "FetchBinary"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/*"]
  }
  statement {
    sid       = "PublishLeaderMetric"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource to scope to
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["jobq"]
    }
  }
}

resource "aws_iam_role" "node" {
  name_prefix        = "${local.name}-node-"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "node" {
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.node.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "node" {
  name_prefix = "${local.name}-node-"
  role        = aws_iam_role.node.name
}

# --- the nodes -----------------------------------------------------------------

resource "aws_instance" "node" {
  for_each = local.nodes

  ami                    = data.aws_ssm_parameter.al2023_arm64.value
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.node[each.key].id
  private_ip             = each.value.private_ip
  vpc_security_group_ids = [aws_security_group.node.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name

  # A new binary or a changed command line replaces the instance, and with
  # it the node's disk. Replace one node at a time and let it catch up
  # before the next: see RUNBOOK.md, "Replacing a node".
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    node_id      = each.key
    private_ip   = each.value.private_ip
    peers        = local.peers
    advertise    = local.advertise
    bucket       = aws_s3_bucket.artifacts.id
    binary_key   = aws_s3_object.jobqd.key
    region       = data.aws_region.current.name
    cluster_name = local.name
  })

  metadata_options {
    http_tokens   = "required" # IMDSv2 only
    http_endpoint = "enabled"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.disk_gb
    encrypted   = true
  }

  lifecycle {
    # A newer Amazon Linux image must not replace three nodes at once.
    ignore_changes = [ami]
  }

  tags = { Name = "${local.name}-${each.key}" }

  depends_on = [
    aws_route_table_association.node, # no route out, no binary
    aws_iam_role_policy.node,
    aws_iam_role_policy_attachment.ssm,
  ]
}

# --- the alarm -----------------------------------------------------------------

# Every node publishes HasLeader every five seconds: 1 if it knows of a
# leader in its term, 0 if not. The alarm looks at the best any node can
# say. If the maximum over ten seconds is below 1, no node knows a leader;
# if there is no data at all, no node is able to say anything. Both are the
# cluster being unable to take writes.
#
# This is only truthful because a leader that loses touch with a majority
# steps down (raft/raft.go). Without that, a cut-off leader would report 1
# for as long as the partition lasted.
resource "aws_sns_topic" "alarms" {
  name_prefix = "${local.name}-alarms-"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email # confirm the subscription from the inbox
}

resource "aws_cloudwatch_metric_alarm" "no_leader" {
  alarm_name          = "${local.name}-no-leader"
  alarm_description   = "No jobq node has known of a leader for 10 seconds: the cluster cannot take writes. See RUNBOOK.md."
  namespace           = "jobq"
  metric_name         = "HasLeader"
  dimensions          = { Cluster = local.name }
  statistic           = "Maximum"
  period              = 10
  evaluation_periods  = 1
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}
