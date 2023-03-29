resource "kubernetes_namespace" "namespace" {
  metadata {
    name = "python-web-${local.environment}"
  }
}
