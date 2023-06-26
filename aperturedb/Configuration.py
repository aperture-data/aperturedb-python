from paprika import *


@to_string
class Configuration:
    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 name: str,
                 use_ssl: bool = True,
                 use_rest: bool = False) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_rest = use_rest
        self.use_ssl = use_ssl
        self.name = name

    def __repr__(self) -> str:
        mode = "REST" if self.use_rest else "TCP"
        return f"[{self.host}:{self.port} as {self.username} using {mode} with SSL={self.use_ssl}]"
