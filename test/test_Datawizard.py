from aperturedb.Constraints import Constraints
from aperturedb.Entities import Entities
import logging

logger = logging.getLogger(__file__)


class TestDataWizardEntities():
    def test_get_persons(self, loader, db):
        loaded = loader(self, "./input/persons.adb.csv")
        all_persons = Entities.retrieve(db, with_class="Person")
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
        df = retired_persons.display()
        logger.info(f"\n{df}")
        ages = df['age']
        assert ages[ages >= 60].count() == len(df)
