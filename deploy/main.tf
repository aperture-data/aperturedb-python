terraform {
  required_providers {
    kubectl = {
      source = "gavinbunney/kubectl"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
    }
    dockerhub = {
      source = "barnabyshearer/dockerhub"
    }
  }
}

locals {
  cluster-name = "web-develop-cluster"
}

data "aws_eks_cluster" "cluster" {
  name = local.cluster-name
}

data "aws_eks_cluster_auth" "cluster" {
  name = data.aws_eks_cluster.cluster.name
}

provider "kubectl" {
  host                   = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.cluster.token
  load_config_file       = false
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
  token                  = data.aws_eks_cluster_auth.cluster.token
}

variable "docker_username" {
  description = "The username of dockerhub account"
  type        = string
}

variable "docker_password" {
  description = "The password of dockerhub account"
  type        = string
}

provider "dockerhub" {
  username = var.docker_username
  password = var.docker_password
}

data "aws_route53_zone" "domain" {
  name = "aperturedata.${var.environment == "main" ? "io" : "dev"}."
}

module "service" {
  source           = "./modules/service"
  for_each         = toset(["coverage"])
  service          = each.key
  namespace        = kubernetes_namespace.namespace.metadata[0].name
  hosted-zone      = data.aws_route53_zone.domain
  dockerhub-secret = kubernetes_secret.dockerhub-token.metadata[0].name
  image-ext-tag    = local.image-ext-tag
}
