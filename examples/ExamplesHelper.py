from typing import Dict
from aperturedb import Connector
import argparse

DB_HOST = "localhost"
DB_PORT = 55557
DB_USER = "admin"
DB_PASSWORD = "admin"


def in_notebook():
    try:
        from IPython import get_ipython
        if 'IPKernelApp' not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


class ExamplesHelper:
    """
    **Common helper methods needed for multiple example programs**
    It is used in notebooks in the examples as well as the command line programs in there.
    This file is kept at the top level, and symlinked to the specefic subfolder.
    """

    def __init__(self, mandatory_params: Dict = {}) -> None:
        if in_notebook():
            class Obj:
                pass
            self.params = Obj()
            self.params.db_host = DB_HOST
            self.params.db_port = DB_PORT
            self.params.db_username = DB_USER
            self.params.db_password = DB_PASSWORD
        else:
            self.params = self.get_args(mandatory_params)

    def create_connector(self):
        return Connector.Connector(
            host=self.params.db_host,
            port=self.params.db_port,
            user=self.params.db_username,
            password=self.params.db_password)

    def get_args(self, mandatory_params: Dict):
        parser = argparse.ArgumentParser()

        # Database config
        parser.add_argument('-db_host', type=str, default=DB_HOST)
        parser.add_argument('-db_port', type=int, default=DB_PORT)
        parser.add_argument('-db_username', type=str, default=DB_USER)
        parser.add_argument('-db_password', type=str, default=DB_PASSWORD)
        for k, v in mandatory_params.items():
            parser.add_argument(
                f"-{k}",
                type=v["type"],
                help=v["help"],
                required=True
            )

        params = parser.parse_args()
        return params
