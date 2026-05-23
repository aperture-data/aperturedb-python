class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True,\
            "Failed to remove all objects"

    def test_get_descriptorset_list(self, utils):
        assert utils.get_descriptorset_list() == []

    def test_create_connector_env(self):
        from aperturedb.Utils import create_connector
        from unittest.mock import patch, MagicMock
        mock_ls = MagicMock(return_value={
            "active": "default",
            "global": {
                "default": MagicMock(),
                "test_env": MagicMock()
            }
        })
        with patch('aperturedb.Utils.ls', mock_ls):
            with patch('aperturedb.Utils.__create_connector') as mock_create:
                with patch('os.environ.get', return_value="test_env"):
                    create_connector()
                    mock_create.assert_called_once_with(mock_ls.return_value["global"]["test_env"])
