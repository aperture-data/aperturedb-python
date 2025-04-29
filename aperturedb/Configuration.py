from dataclasses import dataclass

import json
import re
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
    token: str = None
    user_keys: dict = None

    def __repr__(self) -> str:
        mode = "REST" if self.use_rest else "TCP"
        auth_mode = "token" if self.token is not None else "password"
        return f"[{self.host}:{self.port} as {self.username} using {mode} with SSL={self.use_ssl} auth={auth_mode}]"

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

    def has_user_keys(self) -> bool:
        return self.user_keys is not None

    def add_user_key(self, user, key):
        if self.user_keys is None:
            self.user_keys = dict()
        if not user in self.user_keys:
            self.user_keys[user] = []
        self.user_keys[user].insert(0, key)

    def get_user_key(self, for_user: str) -> str:
        if self.user_keys is None or not (for_user in self.user_keys) \
                or len(self.user_keys[for_user]) == 0:
            return None
        return self.user_keys[for_user][0]

    def set_user_keys(self, keys: dict) -> None:
        self.user_keys = keys

    @classmethod
    def create_web_token(cls, host: str, port: int,  token_string: str) -> None:
        if token_string.startswith("adbp_"):
            token_string = token_string[5:]

        if host.endswith(APERTURE_CLOUD):
            host = host[:-1 * len(APERTURE_CLOUD)]
            m = re.match("(.*)\.farm(\d+)$", host)
            if m is not None:
                host = "{}.{}".format(m.group(1), int(m.group(2)))

            host = "a://{}".format(host)

        if port == 55555:
            web_token_json = [2, host, token_string]
        else:
            web_token_json = [2, host, port, token_string]
        simplified = json.dumps(web_token_json)
        encoded = b64encode(simplified.encode('utf-8')).decode('utf-8')
        return encoded

    @classmethod
    def reinflate(cls, encoded_str: list) -> object:
        try:
            decoded = b64decode(encoded_str.encode('utf-8'))
            as_list = json.loads(decoded.decode('utf-8'))
        except:
            raise Exception(
                "Unable to make configuration from the provided string")
        version = as_list[0]
        if version not in (1, 2):
            raise ValueError("version identifier of configuration was"
                             f"g{as_list[0]}, which is not supported")
        if version == 1:
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
        elif version == 2:
            host = as_list[1]
            if host.startswith("a://"):
                m = re.match("a://([^.]*)\.(\d+)", host)
                host = "{}.farm{:04d}.cloud.aperturedata.io".format(
                    m.group(1), int(m.group(2)))

            cur_arg = 2
            # second arg is port
            if isinstance(as_list[2], int):
                cur_arg = 3
            else:
                port = 55555

            name = "default"

            username = None
            password = None
            token = None
            # if 1 argument left, treat as token.
            if len(as_list) - cur_arg == 1:
                token = "adbp_" + as_list[cur_arg]
            else:
                username = as_list[cur_arg]
                password = as_list[cur_arg + 1]

            c = Configuration(host, port, username,
                              password, name, token = token)
        return c
