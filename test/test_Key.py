import logging

from aperturedb.Configuration import Configuration

logger = logging.getLogger(__name__)

key_pairs = {
    "WzIsOSwibG9jYWxob3N0IixudWxsLCJhZG1pbiIsImFkbWluIl0=":
    [1, 1, "localhost", "admin", "admin"],
        "WzIsOCwiMTI3LjAuMC4xOjU1NTU0IixudWxsLCJZUVp1VlVXZlpvQWRaMmtRRUx4UHlGem1kcndZd3RyMEFEYSJd":
            [1, 0, "127.0.0.1:55554", "YQZuVUWfZoAdZ2kQELxPyFzmdrwYwtr0ADa"],
        "WzIsMTUsIndvcmtmbG93LWxvYWRlZC1mbzFqYXUzdC4wIixudWxsLCJhZG1pbiIsIjEyMzRCVFFMUF8lMnR0Il0=":
            [1, 7, "workflow-loaded-fo1jau3t.farm0000.cloud.aperturedata.io",
                "admin", "1234BTQLP_%2tt"],
        "WzIsMTMsInRlc3QtM3FqcXQ2a3MuNCIsbnVsbCwiWVFadVZVV2Zab0FkWjJrUUVMeFB5RnptZHJ3WXd0cjBBRGEiXQ==":
            [1, 5, "test-3qjqt6ks.farm0004.cloud.aperturedata.io",
                "YQZuVUWfZoAdZ2kQELxPyFzmdrwYwtr0ADa"],
        "WzIsMTAsIjE5Mi4xNjguNC4xMjo1NTU1NSIsbnVsbCwiYWRtaW4iLCJhZG1pbiJd":
        [1, 2, "192.168.4.12:55555", "admin", "admin"],
        "WzIsMTEsImFwZXJ0dXJlZGIuYmlnY29ycC5pbzoxOTE4IixudWxsLCJZUVp1VlVXZlpvQWRaMmtRRUx4UHlGem1kcndZd3RyMEFEYSJd":
        [1, 3, "aperturedb.bigcorp.io:1918", "YQZuVUWfZoAdZ2kQELxPyFzmdrwYwtr0ADa"],
        "WzIsMTIsInRjcC01NTdlcG5uMy45MDk6MTkxOCIsbnVsbCwiYWRtaW4iLCI4OTBFcE1uKyElMiRfIl0=":
        [1, 4, "tcp-557epnn3.farm0909.cloud.aperturedata.io:1918",
            "admin", "890EpMn+!%2$_"],
        "WzIsMTQsImh0dHAtOTBqZzN6cHMuMTI6NDQzIixudWxsLCJZUVp1VlVXZlpvQWRaMmtRRUx4UHlGem1kcndZd3RyMEFEYSJd":
        [1, 6, "http-90jg3zps.farm0012.cloud.aperturedata.io:443",
            "YQZuVUWfZoAdZ2kQELxPyFzmdrwYwtr0ADa"]
}


class TestApertureDBKey():

    def test_encode_keys(self):
        for key, data in key_pairs.items():
            logger.info(f"Testing encoding of {key}")
            config_type = data[1]
            host = data[2]
            username = password = token = None
            comp, rest, ssl, verify_hostname = Configuration.key_type_to_config(
                config_type)
            if host.rfind(':') != -1:
                port = int(host.split(':')[1])
                host = host.split(':')[0]
            else:
                port = Configuration.config_default_port(rest, ssl)
            if len(data) == 4:
                token = data[3]
            else:
                username = data[3]
                password = data[4]
            c = Configuration(host, port, username, password,
                              "encoding test", use_rest=rest, use_ssl=ssl, token=token, verify_hostname=verify_hostname)
            deflated = c.deflate()
            assert deflated == key

    def test_decode_keys(self):
        for key, data in key_pairs.items():
            logger.info(f"Testing decoding of {key}")
            config = Configuration.reinflate(key)
            config_type = data[1]
            host = data[2]
            if config_type == 0 or config_type == 4:
                assert not config.use_rest and not config.use_ssl
            if config_type == 1 or config_type == 5:
                assert not config.use_rest and config.use_ssl
            if config_type == 2 or config_type == 6:
                assert config.use_rest and not config.use_ssl
            if config_type == 3 or config_type == 7:
                assert config.use_rest and config.use_ssl

            if host.rfind(':') != -1:
                port = int(host.split(':')[1])
                host = host.split(':')[0]
                assert config.port == port

            if len(data) == 4:
                assert config.token == "adbp_" + data[3]
            else:
                assert config.username == data[3] and config.password == data[4]

            assert(config.host == host)
