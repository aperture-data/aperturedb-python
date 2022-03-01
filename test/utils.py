from aperturedb import Images, Repository
from aperturedb.BoundingBoxes import BoundingBoxes


    
def cleanDB(db):
    """
    Helper function to reset the DB to clean state.
    """
    images = Images.Images(db)
    entities = Repository.Repository(db)
    bboxes = BoundingBoxes(db)
    print("Cleaning up DB")
    for image in images.filter():
        image.delete()
    for entity in entities.filter():
        entity.delete()
    for bbox in bboxes.filter():
        bbox.delete()