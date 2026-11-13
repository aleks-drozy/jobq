variable "region" {
  description = "AWS region. It needs at least three availability zones."
  type        = string
  default     = "eu-west-1"
}

variable "cluster_name" {
  description = "Names every resource, and is the Cluster dimension of the HasLeader metric."
  type        = string
  default     = "jobq"
}

variable "jobqd_binary" {
  description = "Path to a jobqd built for linux/arm64. Build it with: GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o deploy/terraform/build/jobqd ./cmd/jobqd"
  type        = string
  default     = "build/jobqd"
}

variable "instance_type" {
  description = "Graviton instance type for each node."
  type        = string
  default     = "t4g.small"
}

variable "disk_gb" {
  description = "Root volume size. The Raft log and snapshot live on it."
  type        = number
  default     = 20
}

variable "vpc_cidr" {
  description = "A /16 for the VPC. Each node gets a /24 of it."
  type        = string
  default     = "10.20.0.0/16"
}

variable "client_cidrs" {
  description = "Networks allowed to reach the client API on port 8080, e.g. [\"203.0.113.7/32\"]. The API has no authentication, so keep this to addresses you control. Empty means nodes only; reach them through a Session Manager port forward instead."
  type        = list(string)
  default     = []

  validation {
    condition     = !contains(var.client_cidrs, "0.0.0.0/0")
    error_message = "The client API has no authentication. Opening it to 0.0.0.0/0 gives the queue to the internet."
  }
}

variable "alarm_email" {
  description = "Address to email when the no-leader alarm changes state. Empty means the alarm exists but notifies nobody."
  type        = string
  default     = ""
}
