from aperturedb.ParallelLoader import ParallelLoader


class TestStats:
    def test_stats_all_errors_non_equal_last_batch(self, db, capsys):
        loader = ParallelLoader(db)
        data = [([{"BadCommand": {}}], []) for _ in range(5)]
        loader.ingest(data, batchsize=2, numthreads=1, stats=True)

        assert loader.get_succeeded_queries() == 0
        assert loader.get_succeeded_commands() == 0
        assert "All queries failed!" in capsys.readouterr().out
