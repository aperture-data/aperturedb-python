"""Process workers must be able to log in again after session rejection."""

from dataclasses import replace

import pytest

from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ParallelLoader import ParallelLoader


class RejectedSessionLoader(ParallelLoader):
    def __init__(self, client, dry_run=False):
        super().__init__(client, dry_run=dry_run)
        # Each worker encounters a rejected session before its first write.
        self.client.shared_data.session = replace(
            self.client.shared_data.session,
            session_token="rejected-session", refresh_token="rejected-refresh")


@pytest.mark.dask
def test_process_workers_reauthenticate(db, tmp_path):
    filename = tmp_path / "sessions.csv"
    count = 1000
    filename.write_text("EntityClass,id\n" + "".join(
        f"DaskSessionRecovery,{i}\n" for i in range(count)))
    data = EntityDataCSV(str(filename), use_dask=True)
    data.df = data.df.repartition(npartitions=2).reset_index(drop=True)
    original_session = db.shared_data.session
    loader = RejectedSessionLoader(db)
    # The loader also rejects the parent's token; restore it to check that the
    # worker tasks never share or rotate this session.
    db.shared_data.session = original_session
    try:
        loader.ingest(data, batchsize=100, numthreads=2, stats=False)
        assert loader.error_counter == 0
        assert loader.get_succeeded_commands() == count
        assert db.shared_data.session is original_session
        response, _ = db.query([{"FindEntity": {
            "with_class": "DaskSessionRecovery", "results": {"count": True}}}])
        assert response[0]["FindEntity"]["count"] == count
        assert db.shared_data.session is original_session
    finally:
        if loader.daskManager is not None:
            loader.daskManager._client.close()
            loader.daskManager._cluster.close()
        loader.client.close()
