from aperturedb.Configuration import Configuration


def test_configuration_repr():
    c = Configuration(
        host="localhost",
        port=55555,
        username="admin",
        password="password",
        name="test_config",
        use_ssl=True,
        use_rest=False,
    )
    r = repr(c)
    assert r.startswith("<")
    assert r.endswith(">")
    assert "[" not in r
    assert "]" not in r
    assert "localhost:55555" in r
    assert "as admin" in r
    assert "using TCP" in r
    assert "auth=password" in r
    assert "SSL=" in r
