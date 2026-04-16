provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------

terraform {
  backend "s3" {}
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "region" {
  description = "AWS region to deploy into"
  type        = string
}

variable "ami" {
  description = "AMI for the EC2 instance"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
}

variable "chronicle_service_port" {
  description = "cdb chronicle service port"
  type        = number
}

# ---------------------------------------------------------------------------
# Shared infrastructure (VPC, subnet) from cdb-shared-infra
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "terraform_remote_state" "shared_infra" {
  backend = "s3"

  config = {
    bucket         = "cdb-tf-state-${data.aws_caller_identity.current.account_id}"
    key            = "shared-infra/terraform.tfstate"
    region         = var.region
  }
}

data "terraform_remote_state" "chronicle_log" {
  backend = "s3"

  config = {
    bucket         = "cdb-tf-state-${data.aws_caller_identity.current.account_id}"
    key            = "chronicle-log/terraform.tfstate"
    region         = var.region
  }
}

# ---------------------------------------------------------------------------
# ECR Repository
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "cdb_chronicle_service" {
  name                 = "cdb-chronicle-service"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "cdb-chronicle-service"
  }
}

# ---------------------------------------------------------------------------
# Security Group
# ---------------------------------------------------------------------------

resource "aws_security_group" "cdb_chronicle_service_sg" {
  name        = "cdb-chronicle-service-sg"
  description = "Allow gRPC traffic within VPC"
  vpc_id      = data.terraform_remote_state.shared_infra.outputs.cdb_vpc_id

  ingress {
    description = "Chronicle service gRPC server"
    from_port   = var.chronicle_service_port
    to_port     = var.chronicle_service_port
    protocol    = "tcp"
    cidr_blocks = [data.terraform_remote_state.shared_infra.outputs.cdb_vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "cdb-chronicle-service-sg"
  }
}

# ---------------------------------------------------------------------------
# Build & Push Image to ECR
# ---------------------------------------------------------------------------

resource "null_resource" "build_and_push" {
  triggers = {
    dockerfile_hash = filemd5("Dockerfile")
    pom_hash        = filemd5("pom.xml")
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com
      docker build -t cdb-chronicle-service .
      docker tag cdb-chronicle-service:latest ${aws_ecr_repository.cdb_chronicle_service.repository_url}:latest
      docker push ${aws_ecr_repository.cdb_chronicle_service.repository_url}:latest
    EOT
  }

  depends_on = [aws_ecr_repository.cdb_chronicle_service]
}

# ---------------------------------------------------------------------------
# EC2 Instance
# ---------------------------------------------------------------------------

locals {
  ecr_image_uri = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com/cdb-chronicle-service:latest"
}

resource "aws_instance" "cdb_chronicle_service" {
  ami                         = var.ami
  instance_type               = var.instance_type
  subnet_id                   = data.terraform_remote_state.shared_infra.outputs.cdb_public_subnet_id
  vpc_security_group_ids      = [aws_security_group.cdb_chronicle_service_sg.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.cdb_chronicle_service.name

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Install Docker
    apt-get update -y
    apt-get install -y docker.io unzip
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
    unzip /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install
    systemctl enable docker
    systemctl start docker

    # Install Docker Compose
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # Write docker-compose.yml
    echo "${base64encode(file("docker-compose.yml"))}" | base64 -d > /home/ubuntu/docker-compose.yml

    # Set environment variables
    export ECR_IMAGE_URI="${local.ecr_image_uri}"
    export KAFKA_BOOTSTRAP_SERVERS="${data.terraform_remote_state.chronicle_log.outputs.cdb_chronicle_log_kafka_bootstrap_server}"
    export CHRONICLE_SERVICE_PORT="${var.chronicle_service_port}"

    # Login to ECR and pull image
    aws ecr get-login-password --region ${var.region} \
      | docker login --username AWS --password-stdin \
        ${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.region}.amazonaws.com

    # Start service
    cd /home/ubuntu && docker compose pull && docker compose up -d
  EOF

  depends_on = [null_resource.build_and_push]

  tags = {
    Name = "cdb-chronicle-service"
  }
}

# ---------------------------------------------------------------------------
# IAM Role
# ---------------------------------------------------------------------------

resource "aws_iam_role" "cdb_chronicle_service" {
  name = "cdb-chronicle-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cdb_chronicle_service_ssm" {
  role       = aws_iam_role.cdb_chronicle_service.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "cdb_chronicle_service_ecr" {
  role       = aws_iam_role.cdb_chronicle_service.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_instance_profile" "cdb_chronicle_service" {
  name = "cdb-chronicle-service-profile"
  role = aws_iam_role.cdb_chronicle_service.name
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "cdb_chronicle_service_private_ip" {
  value = aws_instance.cdb_chronicle_service.private_ip
}

output "cdb_chronicle_service_port" {
  value = var.chronicle_service_port
}

output "ecr_repository_url" {
  value = aws_ecr_repository.cdb_chronicle_service.repository_url
}