# This file containts information on to access the server

from aperturedb import Connector

DB_HOST = "localhost"
DB_PORT = 55557
DB_USER = "admin"
DB_PASSWORD = "admin"


def create_connector():
    return Connector.Connector(DB_HOST, DB_PORT, user=DB_USER, password=DB_PASSWORD)
