resource "dockerhub_token" "image-puller" {
  scopes = ["repo:read"]
  label  = "python-web-${local.environment}"
}

resource "kubernetes_secret" "dockerhub-token" {
  metadata {
    namespace = kubernetes_namespace.namespace.metadata[0].name
    name      = "dockerhub-access"
  }
  data = {
    ".dockerconfigjson" = jsonencode({
      auths = {
        "https://index.docker.io/v1/" = {
          auth = base64encode("${var.docker_username}:${dockerhub_token.image-puller.token}")
        }
      }
    })
  }
  type = "kubernetes.io/dockerconfigjson"
}
