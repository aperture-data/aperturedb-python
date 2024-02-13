from dataclasses import dataclass


@dataclass(repr=False)
class Configuration:
    """
    **Configuration object for ApertureDB sdk to be able to connect to ApertureDB**
    """
    host: str
    port: int
    username: str
    password: str
    name: str
    use_ssl: bool = True
    use_rest: bool = False
    use_keepalive: bool = True
    retry_interval_seconds: int = 1
    # Max number of attempts to retry the initial connection (0 means infinite)
    # This is useful when the aperturedb server is not ready yet.
    retry_max_attempts: int = 3

    def __repr__(self) -> str:
        mode = "REST" if self.use_rest else "TCP"
        return f"[{self.host}:{self.port} as {self.username} using {mode} with SSL={self.use_ssl}]"
