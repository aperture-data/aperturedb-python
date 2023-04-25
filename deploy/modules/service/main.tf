terraform {
  required_providers {
    kubectl = {
      source = "gavinbunney/kubectl"
    }
    dockerhub = {
      source = "barnabyshearer/dockerhub"
    }
  }
}

variable "service" {
  description = "The Service Name"
}

variable "namespace" {
  description = "The Namespace to Create The Service within"
}

variable "hosted-zone" {
  description = "The Hosted Zone Serving The Service"
}

variable "dockerhub-secret" {
  description = "The Secret with Dockerhub Credentials to Pull Images"
}

variable "image-ext-tag" {
  description = "The Suffix for Container Images"
}

locals {
  cluster-issuer = "cert-manager-global"
  url            = "python.${var.service}.${var.hosted-zone.name}"
}
