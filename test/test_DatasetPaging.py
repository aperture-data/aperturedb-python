from unittest.mock import MagicMock, patch

import pytest

from aperturedb._dataset_paging import DatasetPaging
from aperturedb.TensorFlowDataset import ApertureDBTensorFlowDataset


def client(version):
    result = MagicMock()
    result.hello_version = version
    result.clone.return_value = result
    result.last_query_ok.return_value = True
    return result


def test_modern_count_and_pages_preserve_the_requested_window():
    original = [{"FindBlob": {"with_class": "Data", "offset": 3, "limit": 5,
                              "results": {"list": ["label"]}, "blobs": True}}]
    paging = DatasetPaging(client(2), original, 0, "FindBlob", 2)
    count = paging.count_query()[0]["FindBlob"]
    assert "offset" not in count and "limit" not in count and "batch" not in count
    assert count["results"] == {"count": True} and count["blobs"] is False
    assert paging.total({"count": 20}) == 5
    paging.page(2, 5)
    assert original[0]["FindBlob"]["offset"] == 7
    assert original[0]["FindBlob"]["limit"] == 1
    assert original[0]["FindBlob"]["results"] == {"list": ["label"]}
    sample = paging.sample_query()[0]["FindBlob"]
    assert sample["offset"] == 3 and sample["limit"] == 1
    assert sample["blobs"] is False and "batch" not in sample


def test_offset_past_end_is_empty():
    paging = DatasetPaging(
        client(2), [{"FindBlob": {"offset": 8}}], 0, "FindBlob", 2)
    assert paging.total({"count": 5}) == 0


def test_legacy_cached_paging_remains_unchanged():
    original = [{"FindBlob": {"blobs": True}}]
    paging = DatasetPaging(client(1), original, 0, "FindBlob", 2)
    assert paging.count_query() is original
    assert original[0]["FindBlob"]["batch"] == {}
    assert paging.total({"batch": {"total_elements": 7}}) == 7
    paging.page(2, 7)
    assert original[0]["FindBlob"]["batch"] == {"batch_size": 2, "batch_id": 2}
    assert "offset" not in original[0]["FindBlob"]


@pytest.mark.parametrize("size", [0, -1, True, 1.5])
def test_invalid_page_size_fails_before_execution(size):
    db = client(2)
    with pytest.raises(ValueError, match="batch_size"):
        DatasetPaging(db, [{"FindBlob": {}}], 0, "FindBlob", size)
    db.query.assert_not_called()


def test_modern_paging_rejects_writes_and_removed_cached_selection():
    with pytest.raises(ValueError, match="read-only"):
        DatasetPaging(client(2), [{"AddBlob": {}}, {
                      "FindBlob": {}}], 1, "FindBlob", 2)
    with pytest.raises(ValueError, match="removed cached"):
        DatasetPaging(
            client(2), [{"FindBlob": {"batch": {"batch_id": 1}}}], 0, "FindBlob", 2)


@pytest.mark.parametrize("version", [1, 2])
def test_dataset_constructor_and_fetch_choose_the_server_api(version):
    db = client(version)
    count = {"count": 4} if version == 2 else {"batch": {"total_elements": 4}}
    replies = [
        (0, [{"FindBlob": {"status": 0, **count}}], []),
        (0, [{"FindBlob": {"status": 0, "entities": [{"label": 2}, {"label": 3}]}}], [
         b"two", b"three"]),
    ]
    with patch("aperturedb.TensorFlowDataset.execute_query", side_effect=replies) as run:
        dataset = ApertureDBTensorFlowDataset(db, [{"FindBlob": {"with_class": "Data"}}],
                                              label_prop="label", batch_size=2)
        dataset.get_batch(2)
    assert dataset.total_elements == 4
    assert dataset.batch_start == 2 and dataset.batch_end == 4
    assert dataset.batch_blobs == [
        b"two", b"three"] and dataset.batch_labels == [2, 3]
    body = run.call_args.kwargs["query"][0]["FindBlob"]
    assert ("batch" in body) == (version == 1)
    if version == 2:
        assert body["offset"] == 2 and body["limit"] == 2


def test_dataset_never_replays_a_failed_query():
    db = client(2)
    with patch("aperturedb.TensorFlowDataset.execute_query", side_effect=[
        (0, [{"FindBlob": {"status": 0, "count": 2}}], []),
        ConnectionError("response interrupted"),
    ]) as run:
        dataset = ApertureDBTensorFlowDataset(
            db, [{"FindBlob": {"with_class": "Data"}}])
        with pytest.raises(ConnectionError, match="interrupted"):
            dataset.get_batch(0)
    assert run.call_count == 2 and db.clone.call_count == 1


def test_forked_worker_clones_before_its_first_query():
    db = client(2)
    with patch("aperturedb.TensorFlowDataset.execute_query", side_effect=[
        (0, [{"FindBlob": {"status": 0, "count": 1}}], []),
        (0, [{"FindBlob": {"status": 0, "entities": [{}]}}], [b"one"]),
    ]):
        dataset = ApertureDBTensorFlowDataset(
            db, [{"FindBlob": {"with_class": "Data"}}])
        with patch("aperturedb.TensorFlowDataset.os.getpid", return_value=dataset._owner_pid + 1):
            dataset.get_batch(0)
    assert db.clone.call_count == 1
    assert dataset.client is not db
    assert dataset.client.config is db.config
    assert dataset.client.shared_data is not db.shared_data
