import os

# This file containts information on to access the server

GATEWAY = os.getenv("GATEWAY", "localhost")

DB_TCP_HOST  = GATEWAY
DB_REST_HOST = GATEWAY
DB_TCP_PORT  = 55556
DB_REST_PORT = 8087
DB_USER      = "admin"
DB_PASSWORD  = "admin"
