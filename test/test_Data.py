import pytest
import numpy as np
import pandas as pd
import os.path as osp
from aperturedb.Query import QueryBuilder, Query
from aperturedb.Entities import Entities
from aperturedb.Constraints import Constraints
from aperturedb.Images import Images
from aperturedb.Utils import Utils

import logging
logger = logging.getLogger(__name__)


@pytest.mark.slow
class TestEntityLoader():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def assertTrue(self, condition):
        if not condition:
            raise AssertionError("Condition not true")

    def test_Loader(self, utils: Utils, insert_data_from_csv):
        classes = ["_Image", "_Descriptor", "Person", "_BoundingBox"]
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())

        results = [utils.create_entity_index(
            e, "id") for e in classes]
        self.assertTrue(all(results))

        # Insert Person Nodes
        data, _ = insert_data_from_csv("./input/persons.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))

        # Insert Images
        data, _ = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

        # Insert Connections
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/connections-persons-images.adb.csv")
        self.assertEqual(
            len(data), utils.count_connections("has_image"))

        # Insert BBoxes
        data, _ = insert_data_from_csv(in_csv_file="./input/bboxes.adb.csv")
        self.assertEqual(len(data), utils.count_bboxes())

    def test_LoaderDescriptorsImages(self, utils, insert_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())

        # Insert Images, images may be there already, and that case should
        # be handled correctly by contraints
        data, _ = insert_data_from_csv(in_csv_file = "./input/images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

        # Insert DescriptorsSet
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/descriptorset.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_DescriptorSet"))

        # Insert Descriptors

        sets = ["setA", "setB"]

        total_descriptors = 0
        for setname in sets:
            data, _ = insert_data_from_csv(
                in_csv_file = "./input/" + setname + ".adb.csv")

            total_descriptors += len(data)
            self.assertEqual(total_descriptors,
                             utils.count_entities("_Descriptor"))

    def test_BlobLoader(self, db, utils, insert_data_from_csv):
        # Assert that we have a clean slate to begin with.
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())

        data, _ = insert_data_from_csv(in_csv_file = "./input/blobs.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Blob"))

        query = [{
            "FindBlob": {
                "blobs": True,
                "results": {
                    "list": ["license"],
                    "limit": 1
                }
            }
        }]

        res, blob = db.query(query)
        self.assertEqual(len(blob), 1)

        arr = np.frombuffer(blob[0])
        self.assertEqual(arr[2], 3.3)

    def test_conditional_add(self, utils, insert_data_from_csv):
        logger.debug(f"Cleaning existing data")
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        # insert one of the images from image csv, for bboxes to refer to.
        images, _ = insert_data_from_csv(
            in_csv_file="./input/images.adb.csv", rec_count=1)
        self.assertEqual(len(images), 1)

        # Insert BBoxes with repeated entries.
        # There is just one unique entry in the input csv.
        logger.debug(f"Inserting bounding box data")
        boxes, _ = insert_data_from_csv(
            in_csv_file="./input/bboxes-constraints.adb.csv")
        self.assertEqual(3, len(boxes))
        self.assertEqual(1, utils.count_bboxes())

    def test_updateif_fails(self, db, utils, modify_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        # verifies loading with empty database.
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))
        # verifies updateif will not update if criteria doesn't pass.
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update-oldversion.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))
        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))
        # if updated, age will be above 200.
        self.assertEqual(
            len(list(filter(lambda p: p['version_id'] == 2, all_persons))), 0)
        self.assertEqual(
            len(list(filter(lambda p: p['age'] >= 200, all_persons))), 0)

    def test_updateif_passes(self, db, utils, modify_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update-newversion.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))
        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))
        # if updated, age will be above 200.
        self.assertEqual(
            len(list(filter(lambda p: p['version_id'] == 2, all_persons))), len(update_data))
        self.assertEqual(
            len(list(filter(lambda p: p['age'] >= 200, all_persons))), len(update_data))

    # Test updating with conditional for > and only some records being updated
    def test_updateif_partial_age(self, db, utils, modify_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update.adb.csv")
        self.assertEqual(len(data), utils.count_entities("Person"))

        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))
        previous_old_age = len(
            list(filter(lambda p: p['age'] > 30, all_persons)))
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/persons-update-olderage.adb.csv")
        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))
        # 200 should be addded to all that were above 30, so both should match.
        new_old_age = len(list(filter(lambda p: p['age'] > 200, all_persons)))
        self.assertEqual(previous_old_age, new_old_age)

    # ensure blob adding works.
    def test_updateif_add_image(self, db, utils, modify_data_from_csv):
        # load images
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_updateif_baseload.adb.csv")
        self.assertEqual(len(data), utils.count_images())
        # load images with partial existance
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_updateif_mixednew.adb.csv")
        self.assertEqual(len(update_data), utils.count_images())

    def get_first_image_size(self, file):
        size = 0
        df = pd.read_csv(file)
        csv_dir = osp.dirname(file)
        with open(osp.join(csv_dir, df.loc[0, 'filename']), "rb") as f:
            data = f.read()
            size = len(data)
        return size
    # Test that images are immutable.

    def test_updateif_image_immutable(self, db, utils, modify_data_from_csv):
        # load images

        big_img_size = self.get_first_image_size(
            "./input/images_updateif_fail_updates.adb.csv")

        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_updateif_fail_baseload.adb.csv")
        self.assertEqual(len(data), utils.count_images())
        # load images with partial existance
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_updateif_fail_updates.adb.csv")
        # images in the second csv are much bigger than from the first.

        images = Images.retrieve(db,
                                 spec=Query.spec(constraints=Constraints().greaterequal("version_id", 2)))
        images.search()
        # all db images must be less than half the size of a larger changed image.
        # filter will include any larger, we expect 0.
        self.assertEqual(0, len(list(
            filter(lambda imglen: imglen > big_img_size / 2, [len(images.get_image_by_index(
                db_img_id)) for db_img_id in range(len(images.images_ids))])
        )))
        # all images were updated - images with version_id >= 2 is all images in db.
        self.assertEqual(len(images), utils.count_images())
        return

    # ensure ImageForceNewestCSV can create images and then only create the additional ones in mixednew.
    def test_imageforce_load_base(self, db, utils, modify_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_baseload.adb.csv")
        self.assertEqual(len(data), utils.count_images())
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_mixednew.adb.csv")
        self.assertEqual(len(update_data), utils.count_images())

    # ensure ImageForceNewestCSV does not update images when not given a way to see if an image has changed.

    def test_imageforce_load_nonupdate(self, db, utils, modify_data_from_csv):
        big_img_size = self.get_first_image_size(
            "./input/images_updateif_fail_updates.adb.csv")
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_fail_base.adb.csv")
        self.assertEqual(len(data), utils.count_images())
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_fail_updates.adb.csv")
        images = Images.retrieve(db,
                                 spec=Query.spec(constraints=Constraints().greaterequal("version_id", 2)))
        images.search()
        # all db images must be less than half the size of a larger changed image.
        # filter will include any larger, we expect 0.
        self.assertEqual(0, len(list(
            filter(lambda imglen: imglen > big_img_size / 2, [len(images.get_image_by_index(
                db_img_id)) for db_img_id in range(len(images.images_ids))])
        )))
        # all images were updated
        self.assertEqual(len(images), utils.count_images())

    def test_imageforce_update(self, db, utils, modify_data_from_csv):
        big_img_size = self.get_first_image_size(
            "./input/images_updateif_fail_updates.adb.csv")
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_blob_baseload.adb.csv")
        self.assertEqual(len(data), utils.count_images())
        update_data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_forceupdate_updates.adb.csv")
        images = Images.retrieve(db,
                                 spec=Query.spec(constraints=Constraints().greaterequal("version_id", 2)))
        images.search()
        # all images were updated
        self.assertEqual(len(update_data), len(list(
            filter(lambda imglen: imglen > big_img_size / 2, [len(images.get_image_by_index(
                db_img_id)) for db_img_id in range(len(images.images_ids))])
        )))
        self.assertEqual(len(images), utils.count_images())

    def test_image_sparse(self, db, utils, modify_data_from_csv):
        self.assertTrue(utils.remove_all_indexes())
        self.assertTrue(utils.remove_all_objects())
        data, _ = modify_data_from_csv(
            in_csv_file = "./input/images_sparseload_base.adb.csv")

        self.assertEqual(len(data), utils.count_images())
        fulldata, _ = modify_data_from_csv(
            in_csv_file = "./input/images_sparseload_full.adb.csv")
        self.assertEqual(len(data) * 2, utils.count_images())


# test EntityUpdateCSV with Person entity.


class TestUpdatePersonEntityCSV():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def assertTrue(self, condition):
        if not condition:
            raise AssertionError("Condition not true")
    # verifies adding and no update when version isn't greater.


# Test functionality of ImageUpdateCSV loader.
@pytest.mark.skip(reason="Old csv not generated")
class TestImageUpdateCSV():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def assertTrue(self, condition):
        if not condition:
            raise AssertionError("Condition not true")

    def test_images(self, utils, modify_data_from_csv):
        data = modify_data_from_csv(
            in_csv_file= "./input/images_update_and_add.adb.csv")
        self.assertEqual(True, True)  # how to verify.

# Test functionality of ImageForceNewestCSV loader.


@pytest.mark.skip(reason="Old csv not generated")
class TestImageForceNewestCSV():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def assertTrue(self, condition):
        if not condition:
            raise AssertionError("Condition not true")

    def test_images(self, utils, modify_data_from_csv):
        #
        data = modify_data_from_csv(
            in_csv_file= "./input/images_newest_blobs.adb.csv")
        self.assertEqual(True, True)  # how to verify.


@pytest.mark.external_network
@pytest.mark.remote_credentials
class TestURILoader():
    def assertEqual(self, expected, actual):
        if expected != actual:
            raise AssertionError(
                "Expected {}, got {}".format(expected, actual))

    def test_S3ImageLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/s3_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

    def test_HttpImageLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/http_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

    def test_GSImageLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/gs_images.adb.csv")
        self.assertEqual(len(data), utils.count_images())

    def test_S3VideoLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/s3_videos.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Video"))

    def test_HttpVideoLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/http_videos.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Video"))

    def test_GSVideoLoader(self, utils, insert_data_from_csv):
        self.assertEqual(utils.remove_all_indexes(), True)
        self.assertEqual(utils.remove_all_objects(), True)
        data, _ = insert_data_from_csv(
            in_csv_file = "./input/gs_videos.adb.csv")
        self.assertEqual(len(data), utils.count_entities("_Video"))
