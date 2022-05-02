from aperturedb import Connector

DB_HOST = "coco.datasets.aperturedata.io"
DB_PORT = 55555
DB_USER = "admin"
DB_PASS = "admin"


def create_connector():

    return Connector.Connector(DB_HOST, DB_PORT, user=DB_USER, password=DB_PASS)
