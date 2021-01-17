import os
import cv2
import math
import numpy as np
from PIL import Image
from IPython.display import display
from io import BytesIO

import skimage.io as io
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon

IMG_ID_PROP = "_uniqueid"

def check_status(json):

    status = 0
    if (isinstance(json, dict)):
        if ("status" not in json):
            status = check_status(json[list(json.keys())[0]])
        else:
            status = json["status"]
    elif (isinstance(json, (tuple, list))):
        if ("status" not in json[0]):
            status = check_status(json[0])
        else:
            status = json[0]["status"]

    return status

class Constraints(object):

    def __init__(self):

        self.constraints = {}

    def equal(self, key, value):

        self.constraints[key] = ["==", value]

    def greaterequal(self, key, value):

        self.constraints[key] = [">=", value]

    def greater(self, key, value):

        self.constraints[key] = [">", value]

    def lessequal(self, key, value):

        self.constraints[key] = ["<=", value]

    def less(self, key, value):

        self.constraints[key] = ["<", value]

class Operations(object):

    def __init__(self):

        self.operations_arr = []

    def resize(self, width, height):

        op = {
            "type": "resize",
            "width": width,
            "height": height,
        }

        self.operations_arr.append(op)

    def rotate(self, angle, resize=False):

        op = {
            "type": "rotate",
            "angle": angle,
            "resize": resize,
        }

        self.operations_arr.append(op)

class Images(object):

    def __init__(self, db, batch_size=20):

        self.db_connector = db

        self.images        = {}
        self.images_ids    = []
        self.images_bboxes = {}

        self.constraints = None
        self.operations  = None
        self.format      = None
        self.limit       = None

        self.search_result = None

        self.batch_size = batch_size
        self.max_cached_images = 1000
        self.total_cached_images = 0
        self.display_limit = 20

    def total_results(self):

        return len(self.images_ids)

    def get_image_by_index(self, index):

        if index >= len(self.images_ids):
            print("Index is incorrect")
            return

        try:
            uniqueid = self.images_ids[index]

            # If image is not retrieved, go and retrieve the batch
            if not str(uniqueid) in self.images:
                self.retrieve_batch(index)

        except:
            print("Cannot retrieve requested image")

        if self.images[str(uniqueid)] == None:
            print("Image was not retrieved")

        return self.images[str(uniqueid)]

    # This will retrieve the entire batch corresponding to image index.
    def retrieve_batch(self, index):

        # Implement the query to retrieve images
        # for that batch

        total_batches = math.ceil(len(self.images_ids) / self.batch_size)
        batch_id = int(math.floor(index / self.batch_size))

        start = batch_id * self.batch_size
        end   = start + self.batch_size

        q = []

        for idx in range(start, end):

            if idx >= len(self.images_ids):
                break

            find = { "FindImage": {
                    "constraints": {
                        "_uniqueid": ["==", self.images_ids[idx]]
                    }
                }
            }
            q.append(find)

        res, imgs = self.db_connector.query(q)

        status = check_status(res)

        if status != 0:
            print(self.db_connector.get_last_response_str())
            return

        for (idx,i) in zip(range(start, end), range(end - start)):

            if idx >= len(self.images_ids):
                return

            uniqueid = self.images_ids[idx]
            self.images[str(uniqueid)] = imgs[i]

    def get_np_image_by_index(self, index):

        image = self.get_image_by_index(index)
        # Just decode the image from buffer
        nparr = np.fromstring(image, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        return image

    def get_bboxes_by_index(self, index):

        if not self.images_bboxes:
            self.retrieve_bounding_boxes()

        try:
            bboxes = self.images_bboxes[str(self.images_ids[index])]
        except:
            print("Cannot retrieve requested bboxes")

        return bboxes

    def get_bounding_boxes_polygons(self, index):

        self.retrieve_bounding_boxes(index)

        ret_poly = []

        uniqueid = self.images_ids[index]

        q = [ {
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    "_uniqueid": ["==", uniqueid]
                },
                "results": {
                    "blob": False,
                }
            }
        }, {
            "FindEntity": {
                "link": {
                    "ref": 1
                },
                 "results": {
                    "blob": True,
                }
            }
        }]

        res, polygons = self.db_connector.query(q)
        ret_poly.append(polygons)

        uniqueid_str = str(uniqueid)
        self.images_bboxes[uniqueid_str]["polygons"] = polygons

        return polygons

    def display_segmentation(self, index):

        polygons = self.get_bounding_boxes_polygons(index)

        image = self.get_image_by_index(index)

        img_stringIO = BytesIO(image)
        I = io.imread(img_stringIO)

        fig1, ax1 = plt.subplots()
        plt.imshow(I); plt.axis('off')

        for poly in polygons:

            sample = np.frombuffer(poly, dtype=np.float32)

            c = (np.random.random((1, 3))*0.6+0.4).tolist()[0]
            color = []
            color.append(c)

            polygon_points = []
            poly = np.array(sample).reshape((int(len(sample)/2), 2))
            polygon_points.append(Polygon(poly))

            ax = plt.gca()
            ax.set_autoscale_on(False)

            p = PatchCollection(polygon_points, facecolor=color, linewidths=0, alpha=0.4)
            ax.add_collection(p)

            p = PatchCollection(polygon_points, facecolor='none', edgecolors=color, linewidths=2)
            ax.add_collection(p)

    def retrieve_bounding_boxes(self, index):

        self.images_bboxes = {}

        if index > len(self.images_ids):
            print("Index error when retrieving bounding boxes")
            return

        uniqueid = self.images_ids[index]

        q = [ {
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    "_uniqueid": ["==", uniqueid]
                },
                "results": {
                    "blob": False,
                }
            }
        }, {
            "FindBoundingBox": {
                "image": 1,
                "_ref": 2,
                 "results": {
                    "blob": False,
                    "list": ["_coordinates", "category_name"],
                }
            }
        }]

        try:
            res, images = self.db_connector.query(q)

            bboxes = []
            tags   = []
            for bbox in res[1]["FindBoundingBox"]["entities"]:
                bboxes.append(bbox["_coordinates"])
                tags.append(bbox["category_name"])

            uniqueid_str = str(uniqueid)
            self.images_bboxes[uniqueid_str] = {}
            self.images_bboxes[uniqueid_str]["bboxes"] = bboxes
            self.images_bboxes[uniqueid_str]["tags"]   = tags

        except:
            print(self.db_connector.get_last_response_str())


    # A new search will throw away the results of any previous search
    def search(self, constraints=None, operations=None, format=None, limit=None):

        self.constraints = constraints
        self.operations  = operations
        self.format      = format
        self.limit       = limit

        self.images = {}
        self.images_ids = []
        self.images_bboxes = {}

        query = { "FindImage": {} }

        if constraints:
            query["FindImage"]["constraints"] = constraints.constraints

        if operations:
            query["FindImage"]["operations"] = operations.operations_arr

        if format:
            query["FindImage"]["format"] = format

        query["FindImage"]["results"] = {}

        if limit:
            query["FindImage"]["results"]["limit"] = limit

        query["FindImage"]["results"]["list"] = []
        query["FindImage"]["results"]["list"].append(IMG_ID_PROP)

        # Only retrieve images when needed
        query["FindImage"]["results"]["blob"] = False

        response, images = self.db_connector.query([query])

        try:
            entities = response[0]["FindImage"]["entities"]

            for ent in entities:
                self.images_ids.append(ent[IMG_ID_PROP])

        except:
            print("Error with search")

        self.search_result = response

    def get_similar_images(self, set_name, n_neighbors):

        imgs_return = Images(self.db_connector)

        for uniqueid in self.images_ids:

            q = [ {
                "FindImage": {
                    "_ref": 1,
                    "constraints": {
                        "_uniqueid":  ["==", uniqueid]
                    },
                    "results": {
                        "blob": False,
                    }
                }
            },{
                "FindDescriptor": {
                    "set": set_name,
                    "link": {
                        "ref": 1,
                    },
                    "results": {
                        "blob": True
                    }
                }
            }]

            response, blobs = self.db_connector.query(q)

            q = [ {
                "FindDescriptor": {
                    "set": set_name,
                    "k_neighbors": n_neighbors + 1,
                    "_ref": 1,
                    "results": {
                        "list": ["_distance", "_id"],
                        "blob": False
                    }
                }
            }, {
                "FindImage": {
                    "link": {
                        "ref": 1,
                    },
                    "results": {
                        "list": ["_uniqueid"]
                    }
                }
            }]

            response, blobs = self.db_connector.query(q, blobs)

            try:
                entities = response[1]["FindImage"]["entities"]

                for ent in entities[1:]:
                    imgs_return.images_ids.append(ent[IMG_ID_PROP])

            except:
                print("Error with similarity search")

        return imgs_return

    def display(self, show_bboxes=False, show_segmentation=False, limit=None):

        if not limit:
            limit = self.display_limit
            if self.display_limit < len(self.images_ids):
                print("Showing only first", limit, "results.")

        for i in range(len(self.images_ids)):

            if limit == 0:
                break
            limit -= 1

            uniqueid = str(self.images_ids[i])
            image    = self.get_image_by_index(i)

            # Just decode the image from buffer
            nparr = np.fromstring(image, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if show_bboxes:
                if not str(uniqueid) in self.images_bboxes:
                    self.retrieve_bounding_boxes(i)

                bboxes = self.images_bboxes[uniqueid]["bboxes"]
                tags   = self.images_bboxes[uniqueid]["tags"]

                # image = cv2.resize(image, (0,0), fx=1.0,fy=1.0)
                # rgb   = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

                # Draw a rectangle around the faces
                counter = 0
                for coords in bboxes:
                    left   = coords["x"]
                    top    = coords["y"]
                    right  = coords["x"] + coords["w"]
                    bottom = coords["y"] + coords["h"]
                    cv2.rectangle(image, (left, top), (right, bottom),
                                  (0, 255, 0), 2)

                    y = top - 15 if top - 15 > 15 else top + 15

                    cv2.putText(image, tags[counter], (left, y),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0, 255, 0), 2)
                    counter += 1

                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

                fig1, ax1 = plt.subplots()
                plt.imshow(image), plt.axis("off")

            elif show_segmentation:
                self.display_segmentation(i)

            else:

                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

                fig1, ax1 = plt.subplots()
                plt.imshow(image), plt.axis("off")

    def get_props_names(self):

        query = [ {
            "FindImageInfo": {
                "results" : {
                    "list" : [ "*" ]
                }
            }
        }]

        res, images = self.db_connector.query(query)

        try:
            dictio = res[0]["FindImageInfo"]["classes"][0]["VD:IMG"]
            search_key = "VD:"
            props_array = [key for key, val in dictio.items() if not search_key in key]
        except:
            props_array = []
            print("Cannot retrieve properties")

        return props_array

    def get_properties(self, prop_list=[]):

        if len(prop_list) == 0:
            return {}

        return_dictionary = {}

        try:
            for uniqueid in self.images_ids:

                q = [ {
                    "FindImage": {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", uniqueid]
                        },
                        "results": {
                            "blob": False,
                            "list": prop_list
                        }
                    }
                }]

                res, images = self.db_connector.query(q)

                return_dictionary[str(uniqueid)] = res[0]["FindImage"]["entities"][0]
        except:
            print("Cannot retrieved properties")

        return return_dictionary
