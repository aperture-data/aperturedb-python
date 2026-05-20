from unittest.mock import patch, MagicMock
import json
from aperturedb.Utils import Utils


class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True, \
            "Failed to remove all objects"

    def test_remove_all_indexes(self, utils):
        assert utils.remove_all_indexes() == True, \
            "Failed to remove all indexes"

    def test_get_descriptorset_list(self, utils):
        assert utils.get_descriptorset_list() == []


class TestUtilsSummaryNormalization():

    def test_summary_normalization(self):
        # We don't use the 'utils' fixture because it requires a live DB connection
        mock_connector = MagicMock()
        utils = Utils(mock_connector)

        mock_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": {
                        "matched": 10,
                        "properties": {
                            "name": [10, True, "string"]
                        }
                    }
                }
            },
            "connections": {
                "returned": 3,
                "classes": {
                    "Knows": {
                        "matched": 5,
                        "properties": {},
                        "src": "Person",
                        "dst": "Person"
                    },
                    "Likes": {
                        "Likes_1": {
                            "matched": 3,
                            "properties": {},
                            "src": "Person",
                            "dst": "Movie"
                        },
                        "Likes_2": {
                            "matched": 4,
                            "properties": {},
                            "src": "Person",
                            "dst": "Book"
                        }
                    },
                    "Owns": [
                        {
                            "matched": 2,
                            "properties": {},
                            "src": "Person",
                            "dst": "Car"
                        }
                    ]
                }
            }
        }
        mock_status = json.dumps(
            [{"GetStatus": {"version": "1.0", "status": "OK", "info": ""}}])

        with patch.object(utils, 'get_schema', return_value=mock_schema), \
                patch.object(utils, 'status', return_value=mock_status):
            # should not raise
            utils.summary()
