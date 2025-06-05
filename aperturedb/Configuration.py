from dataclasses import dataclass

import json
import re
from base64 import b64encode, b64decode

APERTUREDB_CLOUD = ".cloud.aperturedata.io"
APERTUREDB_KEY_VERSION = 1
FLAG_USE_COMPRESSED_HOST = 4
FLAG_USE_REST = 2
FLAG_USE_SSL = 1
DEFAULT_HTTP_PORT = 80
DEFAULT_HTTPS_PORT = 443
DEFAULT_TCP_PORT = 55555


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
        return self.create_aperturedb_key(self.host, self.port, self.token,
                                          self.use_rest, self.use_ssl, self.username, self.password)

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
    def config_to_key_type(cls, compressed_host: bool,  use_rest: bool, use_ssl: bool):
        return (FLAG_USE_COMPRESSED_HOST if compressed_host else 0) + \
               (FLAG_USE_REST if use_rest else 0) + \
               (FLAG_USE_SSL if use_ssl else 0)

    @classmethod
    def key_type_to_config(cls, key_type: int): \
        return [bool(key_type & FLAG_USE_COMPRESSED_HOST),
                bool(key_type & FLAG_USE_REST),
                bool(key_type & FLAG_USE_SSL)]

    @classmethod
    def config_default_port(cls, use_rest: bool, use_ssl: bool):
        if use_rest:
            return DEFAULT_HTTPS_PORT if use_ssl else DEFAULT_HTTP_PORT
        else:
            return DEFAULT_TCP_PORT

    @classmethod
    def create_aperturedb_key(cls, host: str, port: int,  token_string: str,
                              use_rest: bool, use_ssl: bool, username: str = None, password: str = None) -> None:
        compressed = False
        if token_string is not None and token_string.startswith("adbp_"):
            token_string = token_string[5:]

        if host.endswith(APERTUREDB_CLOUD):
            host = host[:-1 * len(APERTUREDB_CLOUD)]
            m = re.match("(.*)\.farm(\d+)$", host)
            if m is not None:
                host = "{}.{}".format(m.group(1), int(m.group(2)))
                compressed = True

        key_type = cls.config_to_key_type(compressed, use_rest, use_ssl)
        default_port = cls.config_default_port(use_rest, use_ssl)
        if port != default_port:
            host = f"{host}:{port}"
        if token_string is not None:
            key_json = [APERTUREDB_KEY_VERSION, key_type, host, token_string]
        else:
            key_json = [APERTUREDB_KEY_VERSION,
                        key_type, host, username, password]
        simplified = json.dumps(key_json, separators=(',', ':'))
        encoded = b64encode(simplified.encode('utf-8')).decode('utf-8')
        return encoded

    @classmethod
    def reinflate(cls, encoded_str: list) -> object:
        name = "default_key"
        try:
            decoded = b64decode(encoded_str.encode('utf-8'))
            as_list = json.loads(decoded.decode('utf-8'))
        except:
            raise Exception(
                "Unable to make configuration from the provided string")
        version = as_list[0]
        if version not in (APERTUREDB_KEY_VERSION,):
            raise ValueError("version identifier of configuration was"
                             f"{version}, which is not supported")
        is_compressed, use_rest, use_ssl = cls.key_type_to_config(as_list[1])
        host = as_list[2]
        token = username = password = None
        if len(as_list) == 4:
            token = "adbp_" + as_list[3]
        elif len(as_list) == 5:
            username, password = as_list[3:]

        port_match = re.match(".*:(\d+)$", host)
        if port_match is not None:
            port = int(port_match.group(1))
            host = host[:-1 * (len(port_match.group(1)) + 1)]
        else:
            port = cls.config_default_port(use_rest, use_ssl)

        if is_compressed:
            try:
                first, second = host.split('.')
                host = "{}.farm{:04d}{}".format(
                    first, int(second), APERTUREDB_CLOUD)
            except Exception as e:
                raise ValueError(
                    f"Unable to parse compressed host: {host} Error: {e}")

        c = Configuration(
            host, port, username, password, name, use_ssl, use_rest)
        if token:
            c.token = token
        return c
