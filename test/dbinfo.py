from aperturedb import Connector
# This file containts information on to access the server

DB_HOST     = "localhost"
DB_PORT     = 55557
DB_USER     = "admin"
DB_PASSWORD = "admin"


def create_connection():
    return Connector.Connector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
