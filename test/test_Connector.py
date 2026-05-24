from aperturedb.Connector import Connector


class TestConnector:
    def test_check_status_mixed(self):
        # We don't need a real connection, just the check_status method
        connector = Connector(connect=False)

        # Test case 1: all ok
        res1 = [{"FindImage": {"status": 0}}, {"FindImage": {"status": 0}}]
        assert connector.check_status(res1) == 0

        # Test case 2: first is ok, second is negative
        res2 = [{"FindImage": {"status": 0}}, {"FindImage": {"status": -4}}]
        assert connector.check_status(res2) == -4

        # Test case 3: first is negative, second is ok
        res3 = [{"FindImage": {"status": -1}}, {"FindImage": {"status": 0}}]
        assert connector.check_status(res3) == -1

        # Test case 4: deeply nested dict
        res4 = {"FindImage": {"status": 0}}
        assert connector.check_status(res4) == 0

        # Test case 5: nested ok, nested negative
        res5 = [{"AddImage": {"status": 0}}, {"AddEntity": {"status": -3}}]
        assert connector.check_status(res5) == -3

        # Test case 6: multiple keys in a single dict (tests values traversal)
        res6 = {"FindImage": {"status": 0}, "FindEntity": {"status": -4}}
        assert connector.check_status(res6) == -4

    def test_close_partially_initialized(self):
        # Test that close() does not throw if called before self.conn is set.
        # This simulates a failure in __init__ followed by __del__ calling close().
        class UninitializedConnector(Connector):
            def __init__(self):
                # Deliberately avoid calling super().__init__() so self.conn is never set
                pass

        connector = UninitializedConnector()
        # Should not raise AttributeError
        connector.close()
        assert getattr(connector, "connected", True) is False

