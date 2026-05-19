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
        
        utils.status = MagicMock(return_value='[{"GetStatus": {"version": "1.0", "status": "OK", "info": "test"}}]')
        
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
