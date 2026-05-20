class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True, \
            "Failed to remove all objects"

    def test_remove_all_indexes(self, utils):
        assert utils.remove_all_indexes() == True, \
            "Failed to remove all indexes"

    def test_get_descriptorset_list(self, utils):
        assert utils.get_descriptorset_list() == []

    def test_summary_supported_schema_shapes(self):
        from unittest.mock import MagicMock
        from aperturedb.Utils import Utils

        mock_connector = MagicMock()
        mock_connector.clone.return_value = mock_connector
        utils = Utils(mock_connector)

        utils.status = MagicMock(
            return_value='[{"GetStatus": {"version": "1.0", "status": "OK", "info": "test"}}]')

        single_dict_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": {
                        "matched": 10,
                        "properties": {}
                    }
                }
            },
            "connections": {
                "returned": 1,
                "classes": {
                    "Knows": {
                        "src": "Person",
                        "dst": "Person",
                        "matched": 5,
                        "properties": {}
                    }
                }
            }
        }

        list_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": [{
                        "matched": 10,
                        "properties": {}
                    }]
                }
            },
            "connections": {
                "returned": 1,
                "classes": {
                    "Knows": [{
                        "src": "Person",
                        "dst": "Person",
                        "matched": 5,
                        "properties": {}
                    }]
                }
            }
        }

        nested_dict_schema = {
            "entities": {
                "returned": 1,
                "classes": {
                    "Person": {
                        "Person": {
                            "matched": 10,
                            "properties": {}
                        }
                    }
                }
            },
            "connections": {
                "returned": 1,
                "classes": {
                    "Knows": {
                        "Person_Person": {
                            "src": "Person",
                            "dst": "Person",
                            "matched": 5,
                            "properties": {}
                        }
                    }
                }
            }
        }

        for schema in [single_dict_schema, list_schema, nested_dict_schema]:
            utils.get_schema = MagicMock(return_value=schema)
            # Should not raise exception
            utils.summary()

    def test_censor_tokens(self):
        from aperturedb.LoggingUtils import censor_tokens

        # Normal response (no auth)
        response = [{"FindImage": {"status": 0}}]
        assert censor_tokens(response) == response

        # Auth response with tokens
        response = [{
            "Authenticate": {
                "status": 0,
                "session_token": "adbs_1234567890abcdef",
                "refresh_token": "adbr_abcdef1234567890",
                "other_field": "visible"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["Authenticate"]["session_token"] == "adbs_1234...cdef"
        assert censored[0]["Authenticate"]["refresh_token"] == "adbr_abcd...7890"
        assert censored[0]["Authenticate"]["other_field"] == "visible"

        # Short tokens
        response = [{
            "Authenticate": {
                "status": 0,
                "session_token": "adbs_1234",
                "refresh_token": "short"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["Authenticate"]["session_token"] == "adbs_..."
        assert censored[0]["Authenticate"]["refresh_token"] == "..."

        # RefreshToken command
        response = [{
            "RefreshToken": {
                "status": 0,
                "session_token": "adbs_longertokentest"
            }
        }]
        censored = censor_tokens(response)
        assert censored[0]["RefreshToken"]["session_token"] == "adbs_long...test"
