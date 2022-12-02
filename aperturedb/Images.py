from __future__ import annotations
from typing import Dict, Tuple, Union
import cv2
import math
import numpy as np

import matplotlib.pyplot as plt

from aperturedb import Utils
from aperturedb.Entities import Entities, ObjectType, Query
from aperturedb.Connector import Connector
from aperturedb.Constraints import Constraints
from ipywidgets import widgets
from IPython.display import display, HTML
import base64
from io import BytesIO
from PIL import Image
from pandas import DataFrame
import logging
logger = logging.getLogger(__name__)


class Images(Entities):
    db_object = "_Image"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query,
                 with_adjacent: Dict[str, Query] = None) -> Images:
        spec.with_class = cls.db_object

        results = Entities.retrieve(
            db=db, spec=spec, with_adjacent=with_adjacent)

        # A Polygon is only connected to 1 image, and our query is filtered with
        # meta info from polygon, so connect the right image to the polygon
        # That being said, the ordering should be same as that of the first command in the query
        images = results[-1]

        return images

    def inspect(self, use_thumbnails=True) -> Union[Tuple[widgets.IntSlider, widgets.Output], DataFrame]:
        df = super(Images, self).inspect()
        if use_thumbnails:
            sizer = widgets.IntSlider(min=1, max=400, value=100)
            op = widgets.Output()

            def widget_interaction(c):
                def image_base64(im):
                    with BytesIO() as buffer:
                        im.save(buffer, 'jpeg')
                        return base64.b64encode(buffer.getvalue()).decode()

                def image_formatter(im):
                    return f'<img width={c["new"]} style="max-width: 400px" src="data:image/jpeg;base64,{image_base64(im)}" >'
                with op:
                    op.clear_output()
                    display(HTML("<div style='max-width: 100%; overflow: auto;'>" +
                                 df.to_html(
                                     formatters={'thumbnail': image_formatter}, escape=False)
                                 + "</div>")
                            )
            sizer.observe(widget_interaction, 'value')
            return sizer, op

        return df

    def getitem(self, idx):
        item = super().getitem(idx)
        if self.get_image == True:
            buffer = self.get_image_by_index(idx)
            if buffer is not None:
                item['thumbnail'] = Image.fromarray(
                    self.get_np_image_by_index(idx))
        return item

    # DW interface ends

    def __init__(self, db, batch_size=100, response=None, **kwargs):
        super().__init__(db, response)

        self.db_connector = db

        self.images          = {}
        self.images_ids      = []
        self.image_sizes     = []
        self.images_bboxes   = {}
        self.images_polygons = {}

        self.overlays = []

        self.constraints = None
        self.operations  = None
        self.format      = None
        self.limit       = None

        self.search_result = None

        self.adjacent = {}

        self.batch_size = batch_size
        self.total_cached_images = 0
        self.display_limit = 20

        self.img_id_prop     = "_uniqueid"
        self.bbox_label_prop = "_label"
        self.get_image = True

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
                        self.img_id_prop: ["==", self.images_ids[idx]]
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

    def __retrieve_polygons(self, index, constraints, tag_key, tag_format):

        if index > len(self.images_ids):
            print("Index error when retrieving polygons")
            return

        uniqueid = self.images_ids[index]

        query = [{
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    self.img_id_prop: ["==", uniqueid]
                },
                "blobs": False,
            }
        }, {
            "FindPolygon": {
                "image_ref": 1,
                "bounds": True,
                "vertices": True,
            }
        }]

        fpq = query[1]["FindPolygon"]
        if constraints and constraints.constraints:
            fpq["constraints"] = constraints.constraints

        keys_to_add = [tag_key]
        for key in keys_to_add:
            if key == "_label":
                fpq["labels"] = True
            elif key == "_uniqueid":
                fpq["uniqueids"] = True
            elif key == "_area":
                fpq["areas"] = True
            else:
                if "results" not in fpq:
                    fpq["results"] = {}
                fpq_res = fpq["results"]
                if "list" not in fpq_res:
                    fpq_res["list"] = []
                fpq_res["list"].append(key)

        try:
            res, _ = self.db_connector.query(query)

            polygons = []
            bounds   = []
            tags     = []
            polys = res[1]["FindPolygon"]["entities"]
            for poly in polys:
                if tag_key and tag_format:
                    tag = tag_format.format(poly[tag_key])
                    tags.append(tag)
                bounds.append(poly["_bounds"])
                polygons.append(poly["_vertices"])

            self.images_polygons[str(uniqueid)] = {
                "bounds": bounds,
                "polygons": polygons,
                "tags": tags,
            }

        except:
            self.images_polygons[str(uniqueid)] = {
                "bounds": [],
                "polygons": [],
                "tags": []
            }

    def __retrieve_bounding_boxes(self, index):
        # We should fetch all bounding boxes incrementally.
        if self.images_bboxes is None:
            self.images_bboxes = {}

        if index > len(self.images_ids):
            print("Index error when retrieving bounding boxes")
            return

        uniqueid = self.images_ids[index]

        query = [{
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    self.img_id_prop: ["==", uniqueid]
                },
                "blobs": False,
            }
        }, {
            "FindBoundingBox": {
                "image_ref": 1,
                "_ref": 2,
                "blobs": False,
                "coordinates": True,
                "labels": True
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
            raise

    def total_results(self):

        return len(self.images_ids)

    def get_image_by_index(self, index):

        if index >= len(self.images_ids):
            logger.info("Index is incorrect")
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
        if not self.images_bboxes or not str(self.images_ids[index]) in self.images_bboxes:
            # Fetch when not present in the map.
            self.__retrieve_bounding_boxes(index)

        try:
            bboxes = self.images_bboxes[str(self.images_ids[index])]
        except Exception as e:
            print("Cannot retrieve requested bboxes")
            print(e)

        return bboxes

    # A new search will throw away the results of any previous search
    def search(self, constraints=None, operations=None, format=None, limit=None, sort=None):

        self.constraints = constraints
        self.operations  = operations
        self.format      = format
        self.limit       = limit

        self.images = {}
        self.images_ids = []
        self.images_sizes = []
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

    def search_by_property(self, prop_key, prop_values):
        const = Constraints()
        const.is_in(prop_key, prop_values)
        img_sort = {
            "key": prop_key,
            "sequence": prop_values,
        }
        self.search(constraints=const, sort=img_sort)

    def add_polygon_overlay(self, polygons, color=None, alpha=0.4):
        if not color:
            color = self.__random_color()
        self.overlays.append({
            "polygons": polygons,
            "color": color,
            "alpha": alpha
        })

    def add_bbox_overlay(self, polygons, color=None):
        if not color:
            color = self.__random_color()
        self.overlays.append({
            "bbox": polygons,
            "color": color,
        })

    def add_text_overlay(self, text, origin, color=None):
        if not color:
            color = self.__random_color()
        self.overlays.append({
            "text": text,
            "color": color,
            "origin": origin,
        })

    def clear_overlays(self):
        self.overlays = []

    def get_similar_images(self, set_name, n_neighbors):

        imgs_return = Images(self.db_connector)

        for uniqueid in self.images_ids:

            query = [{
                "FindImage": {
                    "_ref": 1,
                    "constraints": {
                        self.img_id_prop:  ["==", uniqueid]
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
                    "uniqueids": True,
                }
            }, {
                "FindImage": {
                    "is_connected_to": {
                        "ref": 1,
                    },
                    "group_by_source": True,
                    "results": {
                        "list": [self.img_id_prop]
                    }
                }
            }]

            response, blobs = self.db_connector.query(query, blobs)

            try:
                descriptors = response[0]["FindDescriptor"]["entities"]
                ordered_descs_ids = [x["_uniqueid"] for x in descriptors]

                # Images are not sorted by distance, so we need to sort them
                # That's why we use "group_by_source":
                # To have a mapping between descriptors and associated images.
                imgs_map = response[1]["FindImage"]["entities"]

                for desc_id in ordered_descs_ids:
                    img_info = imgs_map[desc_id][0]
                    # We could assert here that there is only one image per descriptor.
                    imgs_return.images_ids.append(img_info[self.img_id_prop])

            except Exception as e:
                print("Error with search: {}".format(response))
                print(e)
                print("Error with similarity search")

        return imgs_return

    def __draw_text_with_shadow(self, image, text, origin, color, typeface=cv2.FONT_HERSHEY_SIMPLEX, scale=0.75, thickness=2, shadow_color=None, shadow_radius=4):
        if not shadow_color:
            shadow_color = self.__contrasting_color(color)

        cv2.putText(image, text, origin, typeface, scale,
                    shadow_color, thickness + shadow_radius, cv2.LINE_AA)

        cv2.putText(image, text, origin, typeface,
                    scale, color, thickness, cv2.LINE_AA)

    def __draw_bbox(self, image, bbox, color, thickness=2):

        left   = bbox["x"]
        top    = bbox["y"]
        right  = bbox["x"] + bbox["width"]
        bottom = bbox["y"] + bbox["height"]
        cv2.rectangle(image, (left, top), (right, bottom), color, thickness)

    def __draw_bbox_and_tag(self, image, bbox, tag, deferred_tags):
        RED = (0, 0, 255)

        self.__draw_bbox(image, bbox, RED)

        if tag:
            left   = bbox["x"]
            top    = bbox["y"]

            y = top - 15 if top - 15 > 15 else top + 15

            deferred_tags.append({
                "text": tag,
                "origin": (left, y),
                "color": RED
            })

    def __random_color(self, value=1.0, saturation=0.51):
        i_max = int(np.random.random() * 3)
        i_mid = (i_max + int(np.random.random() * 2) + 1) % 3
        v_max = int(255 * value)
        v_min = int(v_max * saturation)
        v_mid = int(v_min + int(np.random.random() * (v_max - v_min)))
        color = [v_min, v_min, v_min]
        color[i_max] = v_max
        color[i_mid] = v_mid
        return tuple(color)

    def __contrasting_color(self, color, saturation=0.51):
        def contrast(v): return 1 if v < 128 else 0
        cont = (contrast(color[0]), contrast(color[1]), contrast(color[2]))
        n_hi = cont[0] + cont[1] + cont[2]
        if not n_hi:
            return (0, 0, 0)
        n_lo = 3 - n_hi
        coeff = 255 * n_hi / ((n_lo * saturation) + n_hi)
        def desaturate(v): return int(coeff) if v else int(saturation * coeff)
        return (desaturate(cont[0]), desaturate(cont[1]), desaturate(cont[2]))

    def __draw_polygon(self, image, polygon, color, fill_alpha=0.4, thickness=2, shift=8):

        def as_shift_int(v): return int(v * (1 << shift))

        for verts in polygon:
            shift_verts = [[as_shift_int(v) for v in vert] for vert in verts]
            np_verts = np.array(shift_verts, np.int32)
            fill = image.copy()
            cv2.fillPoly(fill, [np_verts], color, cv2.LINE_4, shift)
            cv2.addWeighted(fill, fill_alpha, image, 1 - fill_alpha, 0, image)
            cv2.polylines(image, [np_verts], True, color,
                          thickness, cv2.LINE_4, shift)

    def __draw_polygon_and_tag(self, image, polygon, tag, bounds, deferred_tags):

        color = self.__random_color()

        self.__draw_polygon(image, polygon, color)

        if tag:
            left   = bounds["x"]
            top    = bounds["y"]
            right  = bounds["x"] + bounds["width"]
            FONT_WIDTH = 10
            FONT_HEIGHT = 16

            y = top - FONT_HEIGHT
            x = (left + right - (FONT_WIDTH * len(tag))) // 2
            org = (max(x, 0), max(y, 2 * FONT_HEIGHT))

            deferred_tags.append({
                "text": tag,
                "origin": org,
                "color": color
            })

    def display(self, show_bboxes=False, show_polygons=False, limit=None, polygon_constraints=None, polygon_tag_key="_label", polygon_tag_format="{}"):

        if not limit:
            limit = self.display_limit
            if self.display_limit < len(self.images_ids):
                print("Showing only first", limit, "results.")

        if polygon_constraints:
            show_polygons = True
            self.images_polygons = {}

        for i in range(len(self.images_ids)):

            if limit == 0:
                break
            limit -= 1

            uniqueid = str(self.images_ids[i])
            image    = self.get_image_by_index(i)

            # Just decode the image from buffer
            nparr = np.frombuffer(image, dtype=np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            deferred_tags = []
            if show_bboxes:
                if not str(uniqueid) in self.images_bboxes:
                    self.__retrieve_bounding_boxes(i)

                bboxes = self.images_bboxes[uniqueid]["bboxes"]
                tags   = self.images_bboxes[uniqueid]["tags"]

                # Draw a rectangle around the faces
                for bi in range(len(bboxes)):
                    self.__draw_bbox_and_tag(
                        image, bboxes[bi], tags[bi], deferred_tags)

            if show_polygons:
                if str(uniqueid) not in self.images_polygons:
                    self.__retrieve_polygons(
                        i, polygon_constraints, polygon_tag_key, polygon_tag_format)

                bounds = self.images_polygons[uniqueid]["bounds"] if uniqueid in self.images_polygons else [
                ]
                polygons = self.images_polygons[uniqueid]["polygons"] if uniqueid in self.images_polygons else [
                ]
                tags = self.images_polygons[uniqueid]["tags"] if uniqueid in self.images_polygons else [
                ]

                for pi in range(len(polygons)):
                    self.__draw_polygon_and_tag(image, polygons[pi], tags[pi] if pi < len(
                        tags) else None, bounds[pi], deferred_tags)

            for ovr in self.overlays:
                if "polygons" in ovr:
                    self.__draw_polygon(
                        image, ovr["polygons"], ovr["color"], ovr["alpha"])
                elif "bbox" in ovr:
                    self.__draw_bbox(image, ovr["bbox"], ovr["color"])
                elif "text" in ovr:
                    deferred_tags.append(ovr)

            for tag in deferred_tags:
                self.__draw_text_with_shadow(
                    image, tag["text"], tag["origin"], tag["color"])

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
                            self.img_id_prop: ["==", uniqueid]
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
