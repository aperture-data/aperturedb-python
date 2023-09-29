data "aws_lb" "lb" {
  name = "web-develop"
}

resource "aws_route53_record" "http-service" {
  zone_id = var.hosted-zone.zone_id
  name    = local.url
  type    = "CNAME"
  ttl     = 300
  records = [ data.aws_lb.lb.dns_name ]
}
