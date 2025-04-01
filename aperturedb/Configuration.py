from dataclasses import dataclass

import json
from base64 import b64encode, b64decode

APERTURE_CLOUD = ".cloud.aperturedata.io"
AD_CLOUD_SCHEME = "adbc://"


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

    def deflate(self) -> list:
        deflate_version = 1
        host = self.host
        if host.endswith(APERTURE_CLOUD):
            host = "adbc://{}".format(host[:-1 * len(APERTURE_CLOUD)])
        as_list = [deflate_version, host, self.port, self.username, self.password, self.name, int(self.use_ssl),
                   int(self.use_rest), int(self.use_keepalive),
                   self.retry_interval_seconds, self.retry_max_attempts]
        simplified = json.dumps(as_list)
        encoded_key = b64encode(simplified.encode('utf-8')).decode('utf-8')
        return encoded_key

    @classmethod
    def reinflate(cls, encoded_key: list) -> object:
        decoded_key = b64decode(encoded_key.encode('utf-8'))
        as_list = json.loads(decoded_key.decode('utf-8'))
        if as_list[0] != 1:
            raise ValueError(f"version identifier of configuration was"
                             "{as_list[0]}, which is not supported")
        host, port, username, password, name, use_ssl, \
            use_rest, use_keepalive, retry_interval_seconds, \
            retry_max_attempts = as_list[1:]
        if host.startswith(AD_CLOUD_SCHEME):
            host = host[len(AD_CLOUD_SCHEME):] + APERTURE_CLOUD
        use_ssl = bool(use_ssl)
        use_rest = bool(use_rest)
        use_keepaliave = bool(use_keepalive)
        c = Configuration(
            host, port, username, password, name, use_ssl, use_rest, use_keepalive,
            retry_interval_seconds, retry_max_attempts)
        return c
