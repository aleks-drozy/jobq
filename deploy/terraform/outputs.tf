output "instance_ids" {
  description = "Node ID -> instance ID, for Session Manager."
  value       = { for id, i in aws_instance.node : id => i.id }
}

output "private_addresses" {
  description = "Node ID -> the client address the nodes advertise. Reachable from inside the VPC."
  value       = { for id, n in local.nodes : id => "http://${n.private_ip}:8080" }
}

output "public_addresses" {
  description = "Node ID -> client address from outside. Works only from var.client_cidrs. These change if an instance is stopped and started."
  value       = { for id, i in aws_instance.node : id => "http://${i.public_ip}:8080" }
}

output "jobq_addr" {
  description = "Value for jobq --addr from a machine in var.client_cidrs."
  value       = join(",", [for id, i in aws_instance.node : "http://${i.public_ip}:8080"])
}

output "alarm" {
  description = "The no-leader alarm."
  value       = aws_cloudwatch_metric_alarm.no_leader.alarm_name
}

output "session" {
  description = "Open a shell on node 1."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.node["1"].id}"
}
