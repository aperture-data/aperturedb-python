# ApertureDB Client Python Module

This is the python client module for ApertureDB.

It provides a connector to AperetureDB instances using 
the open source connector for [VDMS](https://github.com/IntelLabs/vdms).

It also implements an Object-Mapper API to interact with 
elements in ApertureDB at the object level.

* Status.py provides helper methods to retrieve information about the db.
* Images.py provides the Object-Mapper for image related objetcs (images, bounding boxes, etc)
* NotebookHelpers.py provides helpers to show images/bounding boxes on Jupyter Notebooks

For more information, visit https://aperturedata.io


## Object Mapper API
This API allows the user to interact with the database with the idea of objects that can be Created, Read, Updated and Deleted through an intutive interface involving these objects.

It supports the following types of Objects in the Database as Python objects:
| Object | Dcumentation |
| ----- | -----|
| Blob||
| BoundingBox || 
| Descriptor||
| DescriptorSet||
| Entity||
| Image||
| Video||


### Examples
**Create an Image**

```
from aperturedb import Connector, Images

db_connector = Connector.Connector(user="admin", password="admin")
images = Images.Images(db_connector)
with open(abs_path, "rb") as instream:
    img_data = instream.read()
    images.create_new(
        properties: {
            description: "A new image API",
            custom_index: 92
        }
    )
    image = image.save()
    # image is an object with _uniqueid and arbitrary properties, and methods.
```

**Retrieve an Image**

```
from aperturedb import Connector, Images

db_connector = Connector.Connector(user="admin", password="admin")
images = Images.Images(db_connector)

image = image.get(id=92)
```

**Connect an Image to BoundingBox**
```
from aperturedb import Connector, Images, BoundingBoxes

db_connector = Connector.Connector(user="admin", password="admin")

bboxes = BoundingBoxes.BoundingBoxes(db_connector)
bbox = bboxes.get(id=34)

images = Images.Images(db_connector)
img = images.get(id=1)

img.connect_BoundingBox(bbox)
```