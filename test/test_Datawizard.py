import logging
from aperturedb.Blobs import Blobs
from aperturedb.BoundingBoxes import BoundingBoxes
from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
from aperturedb.Images import Images
from aperturedb.Query import Query
from aperturedb.Query import generate_add_query
from aperturedb.DataModels import ImageDataModel, IdentityDataModel
import random
from typing import List
from enum import Enum

logger = logging.getLogger(__file__)


class TestDataWizardEntities():
    def test_get_persons(self, insert_data_from_csv, db):
        loaded, _ = insert_data_from_csv(in_csv_file="./input/persons.adb.csv")
        all_persons = Entities.retrieve(db,
                                        spec=Query.spec(with_class="Person"))
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
                                                    constraints=Constraints().greaterequal("age", 60)))

        assert len(persons) == len(retired_persons)
        assert all(p["risk_factor"] == (p["age"] - 60) // 10 for p in persons)


class TestDataWizardImages():
    def test_get_images(self, insert_data_from_csv, db):
        loaded, _ = insert_data_from_csv(
            "./input/images.adb.csv", rec_count=10)
        images = Images.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            images, Images), f"{type(images)} is not an instance of Images"
        logger.info(f"\n{images.inspect()}")
        assert images != None
        assert len(images) <= 10


class TestDataWizardBlobs():
    def test_get_blobs(self, insert_data_from_csv, db):
        loaded, _ = insert_data_from_csv("./input/blobs.adb.csv", rec_count=10)
        blobs = Blobs.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            blobs, Blobs), f"blobs is not an instance of Blobs = {blobs}"
        logger.info(f"\n{blobs.inspect()}")
        assert blobs != None
        assert len(blobs) <= 10


class TestDataWizardBoundingBoxes():
    def test_get_bounding_boxes(self, insert_data_from_csv, db):
        loaded, _ = insert_data_from_csv(
            "./input/images.adb.csv", rec_count=10)
        loaded, _ = insert_data_from_csv(
            "./input/bboxes.adb.csv", rec_count=10)
        bboxes = BoundingBoxes.retrieve(db, Query.spec(limit=10))
        assert isinstance(
            bboxes, BoundingBoxes), f"bboxes is not an instance of BoundingBoxes = {bboxes}"
        logger.info(f"\n{bboxes.inspect()}")
        assert bboxes != None
        assert len(bboxes) <= 10


def make_people(count: int = 1) -> List[object]:
    class Side(Enum):
        RIGHT = 1
        LEFT = 2

    class Finger(IdentityDataModel):
        nail_clean: bool = False

    class Hand(ImageDataModel):
        side: Side = None
        thumb: Finger = None
        fingers: List[Finger] = []

    class Person(IdentityDataModel):
        name: str = ""
        hands: List[Hand] = []
        dominant_hand: Hand = None

    def make_hand(side: Side) -> Hand:
        hand = Hand(side = side, url= "input/images/0079.jpg")
        hand.fingers = [Finger(nail_clean=True) if random.randint(
            0, 1) == 1 else Finger(nail_clean=False) for i in range(5)]
        hand.thumb = hand.fingers[0]
        return hand

    people = []
    for i in range(10):
        person = Person(name=f"adam{i+1}")
        left_hand = make_hand(Side.LEFT)
        right_hand = make_hand(Side.RIGHT)
        person.hands.extend([left_hand, right_hand])
        person.dominant_hand = person.hands[0]
        people.append(person)
    return people


class TestQueryBuilder():
    def test_all_info_preserved(self):
        # this will create 10 people entities.
        # Each person will have 2 hands (+1 connection via dominant hand)
        # Each hand will have 10 fingers (+2 connections via thumb)
        # Everything will be interconnected.
        # Nodes per person = 1 + 3 + 12 = 16 (The commands will be generated with if not found)
        # Connections per person = 3 for hand 12 for fingers
        # Commands = 16 + 15 = 31
        people = make_people(10)
        total_commands = []
        total_blobs = []
        for person in people:
            q, b, current_ref = generate_add_query(person)
            total_commands += q
            total_blobs += b
            # 16 entities have been inserted and referenced
            assert current_ref == 16

        assert len(total_commands) == 310
        assert len(total_blobs) == 20
        assert len(
            list(filter(lambda cmd: "AddConnection" in cmd, total_commands))) == 150
        assert len(
            list(filter(lambda cmd: "AddEntity" in cmd, total_commands))) == 130
        assert len(
            list(filter(lambda cmd: "AddImage" in cmd, total_commands))) == 20
        assert len(
            list(filter(lambda cmd: "FindImage" in cmd, total_commands))) == 10
