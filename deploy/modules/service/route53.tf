
resource "aws_route53_record" "http-service" {
  zone_id = var.hosted-zone.zone_id
  name    = local.url
  type    = "CNAME"
  ttl     = 300
  records = kubernetes_ingress_v1.server.status[0].load_balancer[0].ingress[*].hostname
}

resource "kubectl_manifest" "certificate-request" {
  depends_on = [
    aws_route53_record.http-service,
  ]
  yaml_body = yamlencode({
    apiVersion = "cert-manager.io/v1"
    kind       = "Certificate"
    metadata = {
      namespace = var.namespace
      name      = var.service
    }
    spec = {
      dnsNames = [local.url]
      duration = "2160h0m0s"
      issuerRef = {
        kind = "ClusterIssuer"
        name = local.cluster-issuer
      }
      renewBefore    = "360h0m0s"
      secretName     = var.service
      secretTemplate = {}
      subject        = {}
      usages = [
        "server auth",
        "client auth",
      ]
    }
  })
}
