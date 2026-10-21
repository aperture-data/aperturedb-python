"""Session recovery tests that do not require a running database."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import json
import time

import pytest

from aperturedb.Connector import Connector, Session, UnauthorizedException
from aperturedb.ConnectorRest import ConnectorRest


REJECTED = {"status": -1, "code": "unauthenticated",
            "info": "Session rejected"}
LEGACY_REJECTED = {"status": -1, "info": "Not Authenticated!"}
QUERY = [{"AddEntity": {"class": "SessionRecovery"}}]
SUCCESS = [{"AddEntity": {"status": 0}}]


def session(token="old"):
    return Session(token, "refresh-" + token, 3600, 7200, time.time())


def authenticated(command, token="new"):
    return [{command: {
        "status": 0,
        "session_token": token,
        "refresh_token": "refresh-" + token,
        "session_token_expires_in": 3600,
        "refresh_token_expires_in": 7200,
    }}]


@pytest.fixture(params=[Connector, ConnectorRest], ids=["TCP", "REST"])
def connector(request):
    client = request.param(user="alice", password="password", use_ssl=False)
    client.shared_data.session = session()
    client.authenticated = True
    yield client
    client.close()


@pytest.mark.parametrize("rejection", [REJECTED, LEGACY_REJECTED],
                         ids=["structured", "legacy"])
def test_revoked_session_is_renewed_before_local_expiry(
        connector, monkeypatch, rejection):
    calls = []

    def query(q, blobs=None, try_resume=True):
        command = next(iter(q[0]))
        calls.append((command, connector.shared_data.session.session_token))
        if command == "RefreshToken":
            assert q == [{"RefreshToken": {"refresh_token": "refresh-old"}}]
            assert not try_resume
            return authenticated(command), []
        return (rejection if len(calls) == 1 else SUCCESS), []

    assert connector.shared_data.session.valid()
    monkeypatch.setattr(connector, "_query", query)
    assert connector.query(QUERY) == (SUCCESS, [])
    assert calls == [("AddEntity", "old"), ("RefreshToken", "old"),
                     ("AddEntity", "new")]


@pytest.mark.parametrize("refresh_failure", [
    REJECTED,
    LEGACY_REJECTED,
    [{"RefreshToken": {"status": -1, "info": "Refresh token revoked"}}],
], ids=["structured-error", "legacy-error", "command-error"])
@pytest.mark.parametrize("personal_token", [False, True])
def test_rejected_refresh_logs_in_again(connector, monkeypatch,
                                        refresh_failure, personal_token):
    calls = []
    shared_lock = connector.shared_data.lock
    if personal_token:
        connector.config.username = None
        connector.config.password = ""
        connector.token = "adbp_personal-token"

    def query(q, blobs=None, try_resume=True):
        command = next(iter(q[0]))
        calls.append(command)
        if command == "RefreshToken":
            assert not try_resume
            return refresh_failure, []
        if command == "Authenticate":
            assert not try_resume
            assert connector.shared_data.session is None
            expected = (
                {"token": "adbp_personal-token"} if personal_token else
                {"username": "alice", "password": "password"})
            assert q == [{"Authenticate": expected}]
            return authenticated(command), []
        return (REJECTED if len(calls) == 1 else SUCCESS), []

    monkeypatch.setattr(connector, "_query", query)
    assert connector.query(QUERY) == (SUCCESS, [])
    assert calls == ["AddEntity", "RefreshToken", "Authenticate", "AddEntity"]
    assert connector.shared_data.lock is shared_lock


def test_failed_login_does_not_retry_original_query(connector, monkeypatch):
    calls = []

    def query(q, blobs=None, try_resume=True):
        command = next(iter(q[0]))
        calls.append(command)
        if command == "Authenticate":
            return [{command: {"status": -1, "info": "Password reset"}}], []
        return REJECTED, []

    monkeypatch.setattr(connector, "_query", query)
    with pytest.raises(Exception, match="Password reset"):
        connector.query(QUERY)
    assert calls == ["AddEntity", "RefreshToken", "Authenticate"]
    assert connector.shared_data.session is None


@pytest.mark.parametrize("response", [
    {"status": -1, "info": "Permission denied"},
    {"status": -1, "info": "Transaction failed"},
    {"status": 0, "info": "Not Authenticated!"},
    {"status": 0, "code": "unauthenticated"},
    {"status": -1, "code": "forbidden", "info": "Not Authenticated!"},
    [{"AddEntity": {"status": -1, "code": "unauthenticated"}}],
    [{"AddEntity": {"status": -1, "info": "Not Authenticated!"}}],
    [{"AddEntity": {"status": 0}},
     {"AddEntity": {"status": -1, "info": "Not Authenticated!"}}],
])
def test_only_transaction_authentication_rejection_is_retried(
        connector, monkeypatch, response):
    calls = []

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        return response, []

    monkeypatch.setattr(connector, "_query", query)
    assert connector.query(QUERY) == (response, [])
    assert calls == [QUERY]


@pytest.mark.parametrize("command", ["Authenticate", "RefreshToken"])
@pytest.mark.parametrize("serialized", [False, True], ids=["object", "json"])
@pytest.mark.parametrize("state", ["valid", "expired", "missing"])
def test_explicit_authentication_failure_is_not_retried(
        connector, monkeypatch, command, serialized, state):
    calls = []
    if state == "expired":
        connector.shared_data.session.session_token_ttl = 0
    elif state == "missing":
        connector.shared_data.session = None
    original = connector.shared_data.session
    query = [{command: {}}]
    if serialized:
        query = json.dumps(query)

    def rejected(q, blobs=None, try_resume=True):
        assert not try_resume
        calls.append(q)
        return REJECTED, []

    monkeypatch.setattr(connector, "_query", rejected)
    assert connector.query(query) == (REJECTED, [])
    assert calls == [query]
    assert connector.shared_data.session is original


def test_query_failure_does_not_trigger_session_recovery(
        connector, monkeypatch):
    calls = []

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        raise OSError("Response lost after possible commit")

    monkeypatch.setattr(connector, "_query", query)
    with pytest.raises(OSError, match="possible commit"):
        connector.query(QUERY)
    assert calls == [QUERY]


def test_original_query_is_retried_at_most_once(connector, monkeypatch):
    calls = []

    def query(q, blobs=None, try_resume=True):
        command = next(iter(q[0]))
        calls.append(command)
        if command == "RefreshToken":
            return authenticated(command), []
        return REJECTED, []

    monkeypatch.setattr(connector, "_query", query)
    assert connector.query(QUERY) == (REJECTED, [])
    assert calls == ["AddEntity", "RefreshToken", "AddEntity"]


def test_session_renewed_by_clone_is_reused(connector, monkeypatch):
    calls = []
    clone = connector.clone()

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        assert q == QUERY
        if len(calls) == 1:
            # The other connector refreshed while this query was in flight.
            clone.shared_data.session = session("from-clone")
            return REJECTED, []
        assert connector.shared_data.session.session_token == "from-clone"
        return SUCCESS, []

    monkeypatch.setattr(connector, "_query", query)
    assert connector.query(QUERY) == (SUCCESS, [])
    assert calls == [QUERY, QUERY]
    clone.close()


@pytest.mark.parametrize("expired", [False, True])
def test_concurrent_clones_refresh_shared_session_once(
        connector, monkeypatch, expired):
    clone = connector.clone()
    rejected = connector.shared_data.session
    if expired:
        rejected.session_token_ttl = 0
    barrier = Barrier(2)
    calls = []
    shared_lock = connector.shared_data.lock

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        assert q == [{"RefreshToken": {"refresh_token": "refresh-old"}}]
        assert not try_resume
        return authenticated("RefreshToken"), []

    monkeypatch.setattr(connector, "_query", query)
    monkeypatch.setattr(clone, "_query", query)

    def renew(client):
        barrier.wait(timeout=5)
        client._renew_session(None if expired else rejected)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(renew, client) for client in (connector, clone)]
        for future in futures:
            future.result(timeout=5)

    assert len(calls) == 1
    assert connector.shared_data.session is clone.shared_data.session
    assert connector.shared_data.lock is shared_lock
    clone.close()


def test_concurrent_initial_logins_share_lock(connector, monkeypatch):
    connector.shared_data.session = None
    connector.authenticated = False
    clone = connector.clone()
    shared_lock = connector.shared_data.lock
    barrier = Barrier(2)
    calls = []

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        assert not try_resume
        return authenticated("Authenticate"), []

    monkeypatch.setattr(connector, "_query", query)
    monkeypatch.setattr(clone, "_query", query)

    def login(client):
        barrier.wait(timeout=5)
        client.authenticate(client.shared_data, "alice", "password", "")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(login, client) for client in (connector, clone)]
        for future in futures:
            future.result(timeout=5)

    assert calls == [[{"Authenticate": {
        "username": "alice", "password": "password"}}]]
    assert connector.shared_data.lock is shared_lock
    assert connector.shared_data.session is clone.shared_data.session
    clone.close()


def test_exhausted_renewal_does_not_send_original_query(
        connector, monkeypatch):
    calls = []
    connector.shared_data.session.session_token_ttl = 0
    monkeypatch.setattr(
        "aperturedb.Connector.RENEW_SESSION_RETRY_INTERVAL_SEC", 0)

    def query(q, blobs=None, try_resume=True):
        calls.append(q)
        return {"status": -1, "info": "Unexpected server response"}, []

    monkeypatch.setattr(connector, "_query", query)
    with pytest.raises(UnauthorizedException):
        connector.query(QUERY)
    assert len(calls) == 3
    assert all("RefreshToken" in q[0] for q in calls)


def test_clone_recovers_after_another_clone_failed_login(connector, monkeypatch):
    clone = connector.clone()
    clone.authenticated = True
    calls = []

    def failed_query(q, blobs=None, try_resume=True):
        if "Authenticate" in q[0]:
            return [{"Authenticate": {"status": -1, "info": "Login failed"}}], []
        return REJECTED, []

    monkeypatch.setattr(connector, "_query", failed_query)
    with pytest.raises(Exception, match="Login failed"):
        connector.query(QUERY)
    assert clone.shared_data.session is None

    def successful_query(q, blobs=None, try_resume=True):
        command = next(iter(q[0]))
        calls.append(command)
        if command == "Authenticate":
            return authenticated(command), []
        return SUCCESS, []

    monkeypatch.setattr(clone, "_query", successful_query)
    assert clone.query(QUERY) == (SUCCESS, [])
    assert calls == ["Authenticate", "AddEntity"]
    assert connector.shared_data.session is clone.shared_data.session
    clone.close()


def test_retry_preserves_query_and_blobs(connector, monkeypatch):
    query = [{"AddBlob": {}}]
    blobs = [b"original blob"]
    response = [{"AddBlob": {"status": 0}}]
    calls = []

    def send(q, blob_array=None, try_resume=True):
        if "RefreshToken" in q[0]:
            return authenticated("RefreshToken"), []
        calls.append((q, blob_array))
        assert q is query
        assert blob_array is blobs
        return (REJECTED if len(calls) == 1 else response), []

    monkeypatch.setattr(connector, "_query", send)
    assert connector.query(query, blobs) == (response, [])
    assert len(calls) == 2


@pytest.mark.parametrize("expired", [False, True])
def test_disabled_authentication_does_not_renew(connector, monkeypatch, expired):
    connector.should_authenticate = False
    if expired:
        connector.shared_data.session.session_token_ttl = 0
    original = connector.shared_data.session
    calls = []

    def send(q, blobs=None, try_resume=True):
        assert not try_resume
        calls.append(q)
        return REJECTED, []

    monkeypatch.setattr(connector, "_query", send)
    assert connector.query(QUERY) == (REJECTED, [])
    assert calls == [QUERY]
    assert connector.shared_data.session is original


def test_clone_clearing_session_before_send_does_not_prevent_recovery(
        connector, monkeypatch):
    original_authenticate = connector.authenticate
    calls = []

    def authenticate(*args, **kwargs):
        prepared = original_authenticate(*args, **kwargs)
        # Another clone starts reauthentication after this request is prepared.
        connector.shared_data.session = None
        return prepared

    def send(q, blobs=None, try_resume=True):
        calls.append(q)
        assert q == QUERY
        if len(calls) == 1:
            # The request went out without a token; the clone has since logged in.
            connector.shared_data.session = session("from-clone")
            return REJECTED, []
        assert connector.shared_data.session.session_token == "from-clone"
        return SUCCESS, []

    monkeypatch.setattr(connector, "authenticate", authenticate)
    monkeypatch.setattr(connector, "_query", send)
    assert connector.query(QUERY) == (SUCCESS, [])
    assert calls == [QUERY, QUERY]
