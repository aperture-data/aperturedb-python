import time

from test_Base import TestBase
from aperturedb import Utils

import logging
logger = logging.getLogger(__name__)


class TestUtils(TestBase):

    def test_remove_all_objects(self):

        utils = Utils.Utils(self.create_connection())

        self.assertTrue(utils.remove_all_objects(),
                        "Failed to remove all objects")
