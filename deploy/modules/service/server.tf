locals {
  server-replicas = 1
  server-port     = 80
}

resource "kubernetes_deployment" "server" {
  metadata {
    namespace = var.namespace
    name      = var.service
  }
  spec {
    replicas = local.server-replicas
    selector {
      match_labels = {
        pod = var.service
      }
    }
    template {
      metadata {
        labels = {
          pod = var.service
        }
      }
      spec {
        container {
          name              = var.service
          image             = "aperturedata/aperturedb-python-${var.service}${var.image-ext-tag}"
          image_pull_policy = "Always"
          port {
            container_port = local.server-port
          }
        }
        image_pull_secrets {
          name = var.dockerhub-secret
        }
        restart_policy = "Always"
      }
    }
  }
}

resource "kubernetes_service" "server" {
  depends_on = [
    kubernetes_deployment.server,
  ]
  metadata {
    namespace = var.namespace
    name      = var.service
  }
  spec {
    port {
      port = local.server-port
      name = "http"
    }
    selector = {
      pod = var.service
    }
  }
}

resource "kubernetes_ingress_v1" "server" {
  depends_on = [
    kubernetes_service.server,
  ]
  metadata {
    namespace = var.namespace
    name      = var.service
    annotations = {
      "kubernetes.io/tls-acme"         = true
      "cert-manager.io/cluster-issuer" = local.cluster-issuer
    }
  }
  spec {
    ingress_class_name = "nginx"
    tls {
      hosts       = [local.url]
      secret_name = var.service
    }
    rule {
      host = local.url
      http {
        path {
          path      = "/"
          path_type = "Prefix"
          backend {
            service {
              name = var.service
              port {
                number = local.server-port
              }
            }
          }
        }
      }
    }
  }
}
