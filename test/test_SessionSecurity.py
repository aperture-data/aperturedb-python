"""Exercise session recovery after security changes on the current server."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from uuid import uuid4

import pytest


def execute(client, command, **arguments):
    response, _ = client.query([{command: arguments}])
    assert isinstance(response, list), response
    result = response[0][command]
    assert result["status"] == 0, result
    return result


@pytest.fixture
def user(db):
    name = "sdk_session_" + uuid4().hex
    role = name + "_role"
    execute(db, "CreateRole", name=role, objects={
        "read": True, "create": True, "update": True, "delete": True})
    client = None
    try:
        execute(db, "CreateUser", username=name,
                password="SdkSessionPassword1!", roles=[role])
        client = type(db)(config=replace(
            db.config, username=name, password="SdkSessionPassword1!", token=None))
        execute(client, "AddEntity", **{"class": name})
        yield client, name, role
    finally:
        if client is not None:
            client.close()
            execute(db, "RemoveUser", username=name)
        execute(db, "RemoveRole", name=role)


def test_user_change_recovers_before_local_expiry(db, user):
    client, name, _ = user
    old = client.shared_data.session
    assert old.valid()
    execute(db, "UpdateUser", username=name, email="changed@example.test")
    execute(client, "AddEntity", **{"class": name})
    assert client.shared_data.session is not old
    result = execute(db, "FindEntity", with_class=name,
                     results={"count": True})
    assert result["count"] == 2


def test_role_change_applies_to_existing_connection(db, user):
    client, name, role = user
    execute(db, "UpdateRole", name=role, objects={"create": False})
    response, _ = client.query([{"AddEntity": {"class": name}}])
    assert client.check_status(response) < 0, response
    result = execute(db, "FindEntity", with_class=name,
                     results={"count": True})
    assert result["count"] == 1
    execute(db, "UpdateRole", name=role, objects={"create": True})
    execute(client, "AddEntity", **{"class": name})


def test_personal_token_recovers_then_stops_after_revocation(db, user):
    _, name, _ = user
    token = execute(db, "GenerateToken")["token"]
    execute(db, "UpdateUser", username=name, add_tokens=[token])
    client = type(db)(config=replace(
        db.config, username=None, password="", token=token))
    try:
        execute(client, "AddEntity", **{"class": name})
        old = client.shared_data.session
        execute(db, "UpdateUser", username=name, email="token@example.test")
        execute(client, "AddEntity", **{"class": name})
        assert client.shared_data.session is not old
        execute(db, "UpdateUser", username=name, remove_tokens=[token[:13]])
        with pytest.raises(Exception):
            client.query([{"AddEntity": {"class": name}}])
        assert client.shared_data.session is None
        result = execute(db, "FindEntity", with_class=name,
                         results={"count": True})
        assert result["count"] == 3
    finally:
        client.close()


def test_password_change_does_not_retry_write_with_old_credentials(db, user):
    client, name, _ = user
    execute(db, "UpdateUser", username=name, password="ChangedPassword2!")
    with pytest.raises(Exception):
        client.query([{"AddEntity": {"class": name}}])
    assert client.shared_data.session is None
    result = execute(db, "FindEntity", with_class=name,
                     results={"count": True})
    assert result["count"] == 1
    client.config.password = "ChangedPassword2!"
    execute(client, "AddEntity", **{"class": name})


def test_concurrent_clones_reauthenticate_once(db, user, monkeypatch):
    client, name, _ = user
    clients = [client.clone() for _ in range(4)]
    calls = []
    ready = Barrier(len(clients))
    try:
        for clone in clients:
            original = clone._query

            def record(q, *args, _original=original, **kwargs):
                command = next(iter(q[0]))
                if command in ("Authenticate", "RefreshToken"):
                    calls.append(command)
                return _original(q, *args, **kwargs)

            monkeypatch.setattr(clone, "_query", record)
        execute(db, "UpdateUser", username=name, email="clones@example.test")

        def write(clone):
            ready.wait(timeout=10)
            return execute(clone, "AddEntity", **{"class": name})

        with ThreadPoolExecutor(max_workers=len(clients)) as pool:
            futures = [pool.submit(write, clone) for clone in clients]
            for future in futures:
                future.result(timeout=20)
        assert calls == ["RefreshToken", "Authenticate"]
        result = execute(db, "FindEntity", with_class=name,
                         results={"count": True})
        assert result["count"] == 1 + len(clients)
    finally:
        for clone in clients:
            clone.close()
