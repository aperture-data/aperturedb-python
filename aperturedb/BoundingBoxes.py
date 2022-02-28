from aperturedb.Repository import Repository


class BoundingBoxes(Repository):
    def __init__(self, db, batch_size=100):
        super().__init__(db)
        self._object_type = "BoundingBox"