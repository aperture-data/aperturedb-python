from dataclasses import dataclass


@dataclass(repr=False)
class Configuration:
    host: str
    port: int
    username: str
    password: str
    name: str
    use_ssl: bool = True
    use_rest: bool = False

    def __repr__(self) -> str:
        mode = "REST" if self.use_rest else "TCP"
        return f"[{self.host}:{self.port} as {self.username} using {mode} with SSL={self.use_ssl}]"
