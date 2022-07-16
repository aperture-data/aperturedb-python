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

from aperturedb import Utils


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

    def is_in(self, key, val_array):

        self.constraints[key] = ["in", val_array]


class Operations(object):

    def __init__(self):

        self.operations_arr = []

    def get_operations_arr(self):
        return self.operations_arr

    def resize(self, width, height):

        op = {
            "type": "resize",
            "width":  width,
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

    def __init__(self, db, batch_size=100):

        self.db_connector = db

        self.images          = {}
        self.images_ids      = []
        self.images_bboxes   = {}
        self.images_polygons = {}

        self.overlays = []

        self.constraints = None
        self.operations  = None
        self.format      = None
        self.limit       = None

        self.search_result = None

        self.batch_size = batch_size
        self.total_cached_images = 0
        self.display_limit = 20

        self.img_id_prop     = "_uniqueid"
        self.bbox_label_prop = "_label"

    def __retrieve_batch(self, index):
        '''
        **Retrieve the batch that contains the image with the specified index**
        '''

        # Implement the query to retrieve images
        # for that batch

        total_batches = math.ceil(len(self.images_ids) / self.batch_size)
        batch_id      = int(math.floor(index / self.batch_size))

        start = batch_id * self.batch_size
        end   = min(start + self.batch_size, len(self.images_ids))

        query = []

        for idx in range(start, end):

            find = {
                "FindImage": {
                    "constraints": {
                        "_uniqueid": ["==", self.images_ids[idx]]
                    },
                }
            }

            if self.operations and len(self.operations.operations_arr) > 0:
                find["FindImage"]["operations"] = self.operations.operations_arr
            query.append(find)

        res, imgs = self.db_connector.query(query)

        if not self.db_connector.last_query_ok():
            print(self.db_connector.get_last_response_str())
            return

        for (idx, i) in zip(range(start, end), range(end - start)):

            if idx >= len(self.images_ids):
                return

            uniqueid = self.images_ids[idx]
            self.images[str(uniqueid)] = imgs[i]

    def __retrieve_polygons(self, index, in_uids=None, in_tags=None):

        if index > len(self.images_ids):
            print("Index error when retrieving polygons")
            return

        uniqueid = self.images_ids[index]

        query = [{
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    "_uniqueid": ["==", uniqueid]
                },
                "blobs": False,
            }
        }, {
            "FindPolygon": {
                "image_ref": 1,
                "bounds": True,
                "vertices": True,
                "labels": True,
                "uniqueids": True,
            }
        }]

        try:
            res, _ = self.db_connector.query(query)

            polygons = []
            bounds   = []
            tags     = []
            for poly in res[1]["FindPolygon"]["entities"]:
                tag = poly["_label"]
                if in_uids is not None:
                    uid = poly["_uniqueid"]
                    if uid not in in_uids:
                        continue
                    if in_tags is not None:
                        poly_idx = in_uids.index(uid)
                        tag = in_tags[poly_idx]

                tags.append(tag)
                bounds.append(poly["_bounds"])
                polygons.append(poly["_vertices"])

            uniqueid_str = str(uniqueid)
            self.images_polygons[uniqueid_str] = {
                "bounds": bounds,
                "polygons": polygons,
                "tags": tags,
            }

        except:
            print(self.db_connector.get_last_response_str())

    def __retrieve_bounding_boxes(self, index):

        self.images_bboxes = {}

        if index > len(self.images_ids):
            print("Index error when retrieving bounding boxes")
            return

        uniqueid = self.images_ids[index]

        query = [{
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    "_uniqueid": ["==", uniqueid]
                },
                "blobs": False,
            }
        }, {
            "FindBoundingBox": {
                "image": 1,
                "_ref": 2,
                "blobs": False,
                "coordinates": True,
                "labels": True,
                "bounds": True,
            }
        }]

        try:
            res, images = self.db_connector.query(query)

            bboxes = []
            tags   = []
            for bbox in res[1]["FindBoundingBox"]["entities"]:
                bboxes.append(bbox["_coordinates"])
                tags.append(bbox[self.bbox_label_prop])

            uniqueid_str = str(uniqueid)
            self.images_bboxes[uniqueid_str] = {}
            self.images_bboxes[uniqueid_str]["bboxes"] = bboxes
            self.images_bboxes[uniqueid_str]["tags"]   = tags

        except:
            print(self.db_connector.get_last_response_str())

    def total_results(self):

        return len(self.images_ids)

    def get_image_by_index(self, index):

        if index >= len(self.images_ids):
            print("Index is incorrect")
            return

        uniqueid = self.images_ids[index]

        # If image is not retrieved, go and retrieve the batch
        if not str(uniqueid) in self.images:
            self.__retrieve_batch(index)

        if self.images[str(uniqueid)] == None:
            print("Image was not retrieved")

        return self.images[str(uniqueid)]

    def get_np_image_by_index(self, index):

        image = self.get_image_by_index(index)
        # Just decode the image from buffer
        nparr = np.frombuffer(image, dtype=np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        return image

    def get_bboxes_by_index(self, index):

        if not self.images_bboxes:
            self.__retrieve_bounding_boxes(index)

        try:
            bboxes = self.images_bboxes[str(self.images_ids[index])]
        except:
            print("Cannot retrieve requested bboxes")

        return bboxes

    # A new search will throw away the results of any previous search

    def search(self, constraints=None, operations=None, format=None, limit=None, sort=None):

        self.constraints = constraints
        self.operations  = operations
        self.format      = format
        self.limit       = limit

        self.images = {}
        self.images_ids = []
        self.images_bboxes = {}
        self.images_polygons = {}

        self.overlays = []

        query = {"FindImage": {}}

        if constraints:
            query["FindImage"]["constraints"] = constraints.constraints

        if format:
            query["FindImage"]["as_format"] = format

        query["FindImage"]["results"] = {}

        if limit:
            query["FindImage"]["results"]["limit"] = limit

        if sort:
            query["FindImage"]["results"]["sort"] = sort

        query["FindImage"]["results"]["list"] = []
        query["FindImage"]["results"]["list"].append(self.img_id_prop)

        # Only retrieve images when needed
        query["FindImage"]["blobs"] = False

        response, images = self.db_connector.query([query])

        try:
            entities = response[0]["FindImage"]["entities"]

            for ent in entities:
                self.images_ids.append(ent[self.img_id_prop])
        except:
            print("Error with search: {}".format(response))

        self.search_result = response

    def search_by_id(self, ids, id_key="id"):
        const = Constraints()
        const.is_in(id_key, ids)
        img_sort = {
            "key": id_key,
            "sequence": ids,
        }
        self.search(constraints=const, sort=img_sort)

    def add_overlay(self, polygons, color=None):
        if not color:
            color = self.__random_color()
        self.overlays.append({
            "polygons": polygons,
            "color": color,
        })

    def get_similar_images(self, set_name, n_neighbors):

        imgs_return = Images(self.db_connector)

        for uniqueid in self.images_ids:

            query = [{
                "FindImage": {
                    "_ref": 1,
                    "constraints": {
                        "_uniqueid":  ["==", uniqueid]
                    },
                    "blobs": False,
                }
            }, {
                "FindDescriptor": {
                    "set": set_name,
                    "is_connected_to": {
                        "ref": 1,
                    },
                    "blobs": True
                }
            }]

            response, blobs = self.db_connector.query(query)

            query = [{
                "FindDescriptor": {
                    "_ref": 1,
                    "set": set_name,
                    "k_neighbors": n_neighbors + 1,
                    "blobs":     False,
                    "distances": True,
                    "ids":       True,
                }
            }, {
                "FindImage": {
                    "is_connected_to": {
                        "ref": 1,
                    },
                    "results": {
                        "list": ["_uniqueid"]
                    }
                }
            }]

            response, blobs = self.db_connector.query(query, blobs)

            try:
                entities = response[1]["FindImage"]["entities"]

                for ent in entities[1:]:
                    imgs_return.images_ids.append(ent[self.img_id_prop])

            except:
                print("Error with similarity search")

        return imgs_return

    def __draw_text_with_shadow(self, image, text, origin, color, shadow_radius=3, shadow_color=(0,0,0)):
        shadow_orgs = [
            (origin[0]-shadow_radius, origin[1]-shadow_radius),
            (origin[0]+shadow_radius, origin[1]-shadow_radius),
            (origin[0]+shadow_radius, origin[1]+shadow_radius),
            (origin[0]-shadow_radius, origin[1]+shadow_radius),
        ]

        for org in shadow_orgs:
            cv2.putText(image, text, origin, cv2.FONT_HERSHEY_SIMPLEX, 0.75, shadow_color, shadow_radius, cv2.LINE_AA)

        cv2.putText(image, text, origin, cv2.FONT_HERSHEY_SIMPLEX, 0.75, color, 2, cv2.LINE_AA)


    def __draw_bbox_and_tag(self, image, bbox, tag):
        RED = (0, 0, 255)

        left   = bbox["x"]
        top    = bbox["y"]
        right  = bbox["x"] + bbox["width"]
        bottom = bbox["y"] + bbox["height"]
        cv2.rectangle(image, (left, top), (right, bottom), RED, 2)

        y = top - 15 if top - 15 > 15 else top + 15

        self.__draw_text_with_shadow(image, tag, (left, y), RED)

    def __random_color(self):
        return (np.random.random((1, 3)) * 155 + 100).tolist()[0]

    def __draw_polygon(self, image, polygon, color, fill_alpha=0.4, shift=8):

        as_shift_int = lambda v : int(v * (1 << shift))

        for verts in polygon:
            shift_verts = [[as_shift_int(v) for v in vert] for vert in verts]
            np_verts = np.array(shift_verts, np.int32)
            fill = image.copy()
            cv2.fillPoly(fill, [np_verts], color, cv2.LINE_4, shift)
            cv2.addWeighted(fill, fill_alpha, image, 1 - fill_alpha, 0, image)
            cv2.polylines(image, [np_verts], True, color, 2, cv2.LINE_4, shift)

    def __draw_polygon_and_tag(self, image, polygon, tag, bounds):

        color = self.__random_color()

        self.__draw_polygon(image, polygon, color)

        left   = bounds["x"]
        top    = bounds["y"]
        right  = bounds["x"] + bounds["width"]
        FONT_WIDTH = 10
        FONT_HEIGHT = 16

        y = top - FONT_HEIGHT
        x = (left + right - (FONT_WIDTH * len(tag))) // 2
        org = (max(x,0), max(y,2*FONT_HEIGHT))

        self.__draw_text_with_shadow(image, tag, org, color)

    def display(self, show_bboxes=False, show_polygons=False, limit=None, polygon_uids=None, polygon_tags=None):

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
            nparr = np.frombuffer(image, dtype=np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            for ovr in self.overlays:
                self.__draw_polygon(image, ovr["polygons"], ovr["color"])

            if show_bboxes:
                if not str(uniqueid) in self.images_bboxes:
                    self.__retrieve_bounding_boxes(i)

                bboxes = self.images_bboxes[uniqueid]["bboxes"]
                tags   = self.images_bboxes[uniqueid]["tags"]

                # image = cv2.resize(image, (0,0), fx=1.0,fy=1.0)
                # rgb   = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

                # Draw a rectangle around the faces
                for bi in range(len(bboxes)):
                    self.__draw_bbox_and_tag(image, bboxes[bi], tags[bi])

            if show_polygons:
                if not str(uniqueid) in self.images_polygons:
                    self.__retrieve_polygons(i, polygon_uids, polygon_tags)

                bounds = self.images_polygons[uniqueid]["bounds"]
                polygons = self.images_polygons[uniqueid]["polygons"]
                tags = self.images_polygons[uniqueid]["tags"]

                for pi in range(len(polygons)):
                    self.__draw_polygon_and_tag(image, polygons[pi], tags[pi], bounds[pi])

            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            fig1, ax1 = plt.subplots()
            plt.imshow(image), plt.axis("off")

    def get_props_names(self):

        dbutils = Utils.Utils(self.db_connector)
        schema = dbutils.get_schema()

        try:
            dictio = schema["entities"]["classes"]["_Image"]["properties"]
            props_array = [key for key, val in dictio.items()]
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

                query = [{
                    "FindImage": {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", uniqueid]
                        },
                        "blobs": False,
                        "results": {
                            "list": prop_list
                        }
                    }
                }]

                res, images = self.db_connector.query(query)

                return_dictionary[str(
                    uniqueid)] = res[0]["FindImage"]["entities"][0]
        except:
            print("Cannot retrieved properties")

        return return_dictionary
