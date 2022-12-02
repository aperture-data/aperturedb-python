import logging
from aperturedb.Blobs import Blobs
from aperturedb.BoundingBoxes import BoundingBoxes
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Images import Images
from aperturedb.Query import ObjectType, Query

logger = logging.getLogger(__file__)


class TestDataWizardEntities():
    def test_get_persons(self, insert_data_from_csv, db):
        loaded = insert_data_from_csv(in_csv_file="./input/persons.adb.csv")
        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))[0]
        assert len(all_persons) == len(loaded)
        too_young = all_persons.filter(lambda p: p["age"] < 18)
        too_old = all_persons.filter(lambda p: p["age"] > 60)
        working_people = all_persons - too_old - too_young

        assert all([p["age"] >= 18 and p["age"] <= 60 for p in working_people])
        ordered = working_people.sort(key=lambda p: p["age"])
        youngest_working_five = ordered[:5]
        eldest_working_five = ordered[-5:]

        assert len(youngest_working_five) == 5
        assert len(eldest_working_five) == 5
        assert youngest_working_five[0]["age"] < eldest_working_five[0]["age"]

    def test_get_persons_constraints(self, retired_persons):
        logger.info(f"Retired persons count = {len(retired_persons)}")
        assert all(p["age"] >= 60 for p in retired_persons)

    def test_get_persons_display(self, retired_persons):
        logger.info(f"Retired persons count = {len(retired_persons)}")
        df = retired_persons.inspect()
        logger.info(f"\n{df}")
        ages = df['age']
        assert ages[ages >= 60].count() == len(df)

    def test_update_properties(self, db, retired_persons):
        risk_factors = [{
            "risk_factor": (p["age"] - 60) // 10
        } for p in retired_persons]
        retired_persons.update_properties(risk_factors)
        df = retired_persons.inspect()
        logger.info(f"\n{df}")
        persons = Entities.retrieve(db,
                                    spec=Query.spec(with_class="Person",
                                                    constraints=Constraints().greaterequal("age", 60)))[0]

        assert len(persons) == len(retired_persons)
        assert all(p["risk_factor"] == (p["age"] - 60) // 10 for p in persons)


class TestDataWizardImages():
    def test_get_images(self, insert_data_from_csv, db):
        loaded = insert_data_from_csv("./input/images.adb.csv", rec_count=10)
        images = Images.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            images, Images), f"{type(images)} is not an instance of Images"
        logger.info(f"\n{images.inspect()}")
        assert images != None
        assert len(images) <= 10


class TestDataWizardBlobs():
    def test_get_blobs(self, insert_data_from_csv, db):
        loaded = insert_data_from_csv("./input/blobs.adb.csv", rec_count=10)
        blobs = Blobs.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            blobs, Blobs), f"blobs is not an instance of Blobs = {blobs}"
        logger.info(f"\n{blobs.inspect()}")
        assert blobs != None
        assert len(blobs) <= 10


class TestDataWizardBoundingBoxes():
    def test_get_bounding_boxes(self, insert_data_from_csv, db):
        loaded = insert_data_from_csv("./input/bboxes.adb.csv", rec_count=10)
        bboxes = BoundingBoxes.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            bboxes, BoundingBoxes), f"bboxes is not an instance of BoundingBoxes = {bboxes}"
        logger.info(f"\n{bboxes.inspect()}")
        assert bboxes != None
        assert len(bboxes) <= 10
