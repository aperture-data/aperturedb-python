class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True,\
            "Failed to remove all objects"

    def test_get_descriptorset_list(self, utils):
        assert utils.get_descriptorset_list() == []
