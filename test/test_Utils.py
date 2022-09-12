class TestUtils():

    def test_remove_all_objects(self, utils):
        assert utils.remove_all_objects() == True,\
            "Failed to remove all objects"
