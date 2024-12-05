"""
**Image Objects**
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Tuple, Union
import cv2
import math
import numpy as np

import matplotlib.pyplot as plt

from aperturedb import Utils
from aperturedb.Entities import Entities
from aperturedb.Constraints import Constraints
from aperturedb.CommonLibrary import execute_query
from aperturedb.Query import QueryBuilder, ObjectType, class_entity
from ipywidgets import widgets
from IPython.display import display, HTML
import base64
from io import BytesIO
from PIL import Image
from pandas import DataFrame
import logging
logger = logging.getLogger(__name__)


def np_arr_img_to_bytes(arr, format: str = 'JPEG') -> bytes:
    """
    **Convert a NumPy array to bytes**

    Args:
        arr: The NumPy array to convert
        format: The format of the image. Defaults to "JPEG".

    Returns:
        bytes: The image as bytes
    """
    return image_to_bytes(Image.fromarray(arr, 'RGB'), format=format)


def image_to_bytes(image: Image, format: str = "JPEG") -> bytes:
    """
    **Convert an image to bytes**

    Args:
        image (PIL.Image): The image to convert
        format (str, optional): The format of the image. Defaults to "JPEG".

    Returns:
        bytes: The image as bytes
    """
    with BytesIO() as bytes:
        image.save(bytes, format=format)
        return bytes.getvalue()


def rotate(points, angle, c_x=0, c_y=0):
    """
    **Rotate a set of points around a center**

    Args:
        points: The points to rotate; iterable of (x, y) pairs
        angle: The angle of counterclockwise rotation in degrees
        c_x: The x coordinate of the center of rotation
        c_y: The y coordinate of the center of rotation

    Returns:
        points: The rotated points as a NumPy array of shape (n,2) and type int
    """
    ANGLE = np.deg2rad(angle)
    return np.array(
        [
            [
                c_x + np.cos(ANGLE) * (px - c_x) - np.sin(ANGLE) * (py - c_y),
                c_y + np.sin(ANGLE) * (px - c_x) + np.cos(ANGLE) * (py - c_y)
            ]
            for px, py in points
        ]
    ).astype(int)


def resolve(points: np.array, image_meta, operations) -> np.array:
    """
    **Resolve the coordinates of a bounding box to the original image size**

    Given an array of (x,y) points with respect to the coordinates of an image, returns the corresponding coordinates with respect to a the resized or rotated image that resulted from a series of operations.

    Args:
        points (NumPy array): The coordinates of the bounding box; shape (...,2)
        image (dict): The image properties
        operations (list): The operations applied to the image

    Returns:
        resolved_points (NumPy array): The resolved coordinates, same shape as points
    """
    resolved = points.copy()
    if image_meta["adb_image_width"] and image_meta["adb_image_height"]:
        image_meta_width = image_meta["adb_image_width"]
        image_meta_height = image_meta["adb_image_height"]
        for operation in operations:
            if operation["type"] == "resize":
                x_ratio = operation["width"] / image_meta_width
                y_ratio = operation["height"] / image_meta_height
                np.multiply(resolved, [x_ratio, y_ratio],
                            out=resolved, casting="unsafe")
                image_meta_width = operation["width"]
                image_meta_height = operation["height"]
            if operation["type"] == "rotate":
                angle = operation["angle"]
                resolved = rotate(
                    resolved, angle, c_x=image_meta_width / 2, c_y=image_meta_height / 2)
            # print(f"resolve: {resolved}")
    else:
        logger.warn(
            "Cannot resolve bounding box coordinates, missing image metadata.")
        logger.warn(
            "Reingest the image with image_properties transformer for this functionality.")
    return resolved


class Images(Entities):
    """
    **The python wrapper of images in ApertureDB.**

    This class serves 2 purposes:

    **This class is a layer on top of the native query.**

    It facilitate interactions with images in the database in a pythonic way.
    Abstracts the complexity of the query language and the communication with the database.

    **It includes utility methods to visualize image and annotations**

    Inter convert the representation into NumPy matrices and find similar images,
    related bounding boxes, etc.

    Args:
        client: The database connector, perhaps as returned by `CommonLibrary.create_connector`
    """
    db_object = ObjectType.IMAGE

    def inspect(self, use_thumbnails=True) -> Union[Tuple[widgets.IntSlider, widgets.Output], DataFrame]:
        df = super(Images, self).inspect()
        if use_thumbnails:
            def image_base64(im):
                with BytesIO() as buffer:
                    im.save(buffer, 'jpeg')
                    return base64.b64encode(buffer.getvalue()).decode()

            def get_formatters(width):
                def image_formatter(im):
                    return f'<img width={width} style="max-width: 400px" src="data:image/jpeg;base64,{image_base64(im)}" >'

                return image_formatter

            sizer = widgets.IntSlider(min=1, max=400, value=100)
            op = widgets.Output()

            with op:
                image_formatter = get_formatters(sizer.value)
                display(HTML("<div style='max-width: 100%; overflow: auto;'>" +
                             df.to_html(
                                 formatters={'thumbnail': image_formatter}, escape=False)
                             + "</div>")
                        )

            def widget_interaction(c):
                image_formatter = get_formatters(c['new'])
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
            if 'thumbnail' not in item:
                buffer = self.get_image_by_index(idx)
                if buffer is not None:
                    item['thumbnail'] = Image.fromarray(
                        self.get_np_image_by_index(idx))
        return item

    # DW interface ends

    def __init__(self, client, batch_size=100, response=None, **kwargs):
        super().__init__(client, response)

        self.client = client

        self.images = {}
        self.images_ids = []
        self.image_sizes = []
        self.images_bboxes = {}
        self.images_polygons = {}
        self.overlays = []
        self.color_for_tag = {}

        self.constraints = None
        self.operations = None
        self.format = None
        self.limit = None
        self.query = None

        self.adjacent = {}

        self.batch_size = batch_size
        self.total_cached_images = 0
        self.display_limit = 20

        self.img_id_prop = "_uniqueid"
        self.bbox_label_prop = "_label"
        self.get_image = True

        if response is not None:
            self.images_ids = list(
                map(lambda x: x[self.img_id_prop], response))

        # Blobs can be passed in addition the response.
        # This would mean that the images are already retrieved.
        # This is useful for usage of the class for it's utility methods.
        if "blobs" in kwargs:
            blobs = kwargs["blobs"]
            for i, id in enumerate(self.images_ids):
                self.images[id] = blobs[i]

        # If the query is passed, we should save the information
        # for things like operations, constraints, etc.
        # This is useful for resolving the coordinates of bounding boxes.
        if "query" in kwargs:
            self.query = kwargs["query"]

    def __retrieve_batch(self, index):
        '''
        **Retrieve the batch that contains the image with the specified index**
        '''

        # Implement the query to retrieve images
        # for that batch

        total_batches = math.ceil(len(self.images_ids) / self.batch_size)
        batch_id = int(math.floor(index / self.batch_size))

        start = batch_id * self.batch_size
        end = min(start + self.batch_size, len(self.images_ids))

        query = []

        for idx in range(start, end):

            find_params = {
                "constraints": {
                    self.img_id_prop: ["==", self.images_ids[idx]]
                },
                "blobs": True
            }

            if self.operations and len(self.operations.operations_arr) > 0:
                find_params["operations"] = self.operations.operations_arr
            query.append(QueryBuilder.find_command(
                self.db_object, params=find_params))

        _, res, imgs = execute_query(self.client, query, [])

        if not self.client.last_query_ok():
            print(self.client.get_last_response_str())
            return

        for (idx, i) in zip(range(start, end), range(end - start)):

            if idx >= len(self.images_ids):
                return

            uniqueid = self.images_ids[idx]
            self.images[str(uniqueid)] = imgs[i]

    def retrieve_polygons(self, index):
        return self.__retrieve_polygons(index, constraints=None, tag_key="_label", tag_format="{}")

    def __retrieve_polygons(self, index, constraints, tag_key, tag_format):

        if index > len(self.images_ids):
            print("Index error when retrieving polygons")
            return

        uniqueid = self.images_ids[index]

        find_image_params = {
            "_ref": 1,
            "constraints": {
                self.img_id_prop: ["==", uniqueid]
            },
            "blobs": False,
            "results": {
                "list": ["adb_image_width", "adb_image_height"]
            },
        }

        find_poly_params = {
            "image_ref": 1,
            "bounds": True,
            "vertices": True,
        }

        if constraints and constraints.constraints:
            find_poly_params["constraints"] = constraints.constraints

        keys_to_add = [tag_key]
        for key in keys_to_add:
            if key == "_label":
                find_poly_params["labels"] = True
            elif key == "_uniqueid":
                find_poly_params["uniqueids"] = True
            elif key == "_area":
                find_poly_params["areas"] = True
            else:
                if "results" not in find_poly_params:
                    find_poly_params["results"] = {}
                fpq_res = find_poly_params["results"]
                if "list" not in fpq_res:
                    fpq_res["list"] = []
                fpq_res["list"].append(key)

        try:
            polygons = []
            bounds = []
            tags = []
            meta = []
            query = [
                QueryBuilder.find_command(
                    self.db_object, params=find_image_params),
                QueryBuilder.find_command(
                    ObjectType.POLYGON, params=find_poly_params)
            ]
            result, res, _ = execute_query(
                client=self.client, query=query, blobs=[])

            if "entities" in res[1]["FindPolygon"]:
                polys = res[1]["FindPolygon"]["entities"]
                operations = self.query["operations"] if self.query and "operations" in self.query else [
                ]
                FindCommand = "Find" + class_entity(self.db_object)
                for poly in polys:
                    if tag_key and tag_format:
                        tag = tag_format.format(poly[tag_key])
                        tags.append(tag)
                        meta.append(res[0][FindCommand]["entities"][0])

                    bounds.append(poly["_bounds"])
                    converted = []
                    for vert in poly["_vertices"]:
                        v = resolve(
                            np.array(vert),
                            res[0][FindCommand]["entities"][0],
                            operations)
                        converted.append(v)
                    polygons.append(converted)

                self.images_polygons[str(uniqueid)] = {
                    "bounds": bounds,
                    "polygons": polygons,
                    "tags": tags,
                    "meta": meta
                }

        except Exception as e:
            self.images_polygons[str(uniqueid)] = {
                "bounds": [],
                "polygons": [],
                "tags": [],
                "meta": []
            }
            logger.warn(
                f"Cannot retrieve polygons for image {uniqueid}", exc_info=True)

    def __get_box_from_coords(self, coordinates):
        box = np.array([
            (coordinates["x"], coordinates["y"]),
            (coordinates["x"] +
             coordinates["width"], coordinates["y"]),
            (coordinates["x"] + coordinates["width"],
             coordinates["y"] + coordinates["height"]),
            (coordinates["x"], coordinates["y"] + coordinates["height"])]
        )
        return box

    def __retrieve_bounding_boxes(self, index, constraints):
        # We should fetch all bounding boxes incrementally.
        if self.images_bboxes is None:
            self.images_bboxes = {}

        if index > len(self.images_ids):
            print("Index error when retrieving bounding boxes")
            return

        uniqueid = self.images_ids[index]

        find_image_params = {
            "_ref": 1,
            "constraints": {
                self.img_id_prop: ["==", uniqueid]
            },
            "results": {
                "list": ["adb_image_width", "adb_image_height"]
            },
            "blobs": False,
        }

        find_bbox_params = {
            "image_ref": 1,
            "_ref": 2,
            "blobs": False,
            "coordinates": True,
            "labels": True
        }

        query = [
            QueryBuilder.find_command(
                self.db_object, params=find_image_params),
            QueryBuilder.find_command(
                ObjectType.BOUNDING_BOX, params=find_bbox_params),
        ]

        if constraints and constraints.constraints:
            find_bbox_params["constraints"] = constraints.constraints

        uniqueid_str = str(uniqueid)
        self.images_bboxes[uniqueid_str] = {}
        try:
            bboxes = []
            tags = []
            meta = []
            bounds = []
            FindCommand = "Find" + class_entity(self.db_object)
            result, res, images = execute_query(
                client=self.client, query=query, blobs=[])
            if "entities" in res[1]["FindBoundingBox"]:
                for bbox in res[1]["FindBoundingBox"]["entities"]:
                    coordinates = bbox["_coordinates"]
                    box = self.__get_box_from_coords(coordinates)
                    operations = self.query["operations"] if self.query and "operations" in self.query else [
                    ]
                    resolved = resolve(
                        box,
                        # image to bb is 1:n relation
                        res[0][FindCommand]["entities"][0],
                        operations)
                    bboxes.append(resolved)
                    tags.append(bbox[self.bbox_label_prop])
                    meta.append(
                        res[0][FindCommand]["entities"][0])
                    bounds.append(box)
        except Exception as e:
            logger.warn(
                f"Cannot retrieve bounding boxes for {self.get_object_name()}: {uniqueid}", exc_info=True)
        finally:
            self.images_bboxes[uniqueid_str]["bboxes"] = bboxes
            self.images_bboxes[uniqueid_str]["tags"] = tags
            self.images_bboxes[uniqueid_str]["meta"] = meta
            self.images_bboxes[uniqueid_str]["bounds"] = bounds

    def total_results(self) -> int:
        """
        **Returns the total number of images that matched the query**

        Returns:
            int: Count of images that match the query.
        """

        return len(self.images_ids)

    def get_image_by_index(self, index: int):
        """**Get a single image by its index in the array of retrieved ids**

        Args:
            index (int): Position in the image ids retrieved.

        """

        if index >= len(self.images_ids):
            logger.info("Index is incorrect")
            return

        uniqueid = self.images_ids[index]

        # If image is not retrieved, go and retrieve the batch
        if not str(uniqueid) in self.images:
            self.__retrieve_batch(index)

        if self.images[str(uniqueid)] == None:
            print(f"{self.get_object_name()} was not retrieved")

        return self.images[str(uniqueid)]

    def get_np_image_by_index(self, index: int):
        """**Retrieves the NumPy representation of image from database**

        Args:
            index (int): Position in the image ids retrieved.

        """

        image = self.get_image_by_index(index)
        # Just decode the image from buffer
        nparr = np.frombuffer(image, dtype=np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        return image

    def get_bboxes_by_index(self, index: int):
        """
        **Get related bounding box for the image**

        Args:
            index (int): Position in the image ids retrieved.

        Returns:
            _type_: _description_
        """
        if not self.images_bboxes or not str(self.images_ids[index]) in self.images_bboxes:
            # Fetch when not present in the map.
            self.__retrieve_bounding_boxes(index, None)

        try:
            bboxes = self.images_bboxes[str(self.images_ids[index])]
        except Exception as e:
            print("Cannot retrieve requested bboxes")
            print(e)

        return bboxes

    # A new search will throw away the results of any previous search
    def search(self, constraints=None, operations=None, format=None, limit=None, sort=None):
        """
        **Sets a new query to retrieve images parameters as described below**

        Args:
            constraints (dict, optional): [Constraints](/python_sdk/parameter_wrappers/Constraints). Defaults to None.
            operations (dict, optional): [Operations](/python_sdk/parameter_wrappers/Operations). Defaults to None.
            format (str, optional): Format of the returned images. Without this, the images are returned as they are stored. Defaults to None.
            limit (int, optional): number of values to restrict results to. Defaults to None.
            sort (dict, optional): [Sort](/python_sdk/parameter_wrappers/Sort). Defaults to None.
        """

        self.constraints = constraints
        self.operations = operations
        self.format = format
        self.limit = limit

        self.images = {}
        self.images_ids = []
        self.images_sizes = []
        self.images_bboxes = {}
        self.images_polygons = {}
        self.overlays = []
        self.color_for_tag = {}

        find_image_params = {}

        if constraints:
            find_image_params["constraints"] = constraints.constraints

        if format:
            find_image_params["as_format"] = format

        find_image_params["results"] = {}

        if limit:
            find_image_params["results"]["limit"] = limit

        if sort:
            find_image_params["results"]["sort"] = sort

        find_image_params["results"]["list"] = []
        find_image_params["results"]["list"].append(self.img_id_prop)

        # Only retrieve images when needed
        find_image_params["blobs"] = False

        _, response, images = execute_query(
            self.client, query=[
                QueryBuilder.find_command(
                    self.db_object, params=find_image_params)
            ], blobs=[])

        entities = None
        try:
            cmd, = response[0].keys()
            entities = response[0][cmd]["entities"]

            for ent in entities:
                self.images_ids.append(ent[self.img_id_prop])
        except Exception as e:
            print(f"Error with search: {e}\n Response = {response}")

        self.response = entities

    def search_by_property(self, prop_key: str, prop_values: list):
        """
        **Constructs a constraints block and does a new search**

        Args:
            prop_key (str): Key on which to search
            prop_values (list): The values that must match for the key.
        """
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

    def add_bbox_overlay(self, bbox, color=None):
        if not color:
            color = self.__random_color()
        self.overlays.append({
            "bbox": self.__get_box_from_coords(bbox),
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

        imgs_return = Images(self.client)

        for uniqueid in self.images_ids:

            find_image_params = {
                "_ref": 1,
                "constraints": {
                    self.img_id_prop:  ["==", uniqueid]
                },
                "blobs": False,
            }
            find_descriptor_params = {
                "set": set_name,
                "is_connected_to": {
                    "ref": 1,
                },
                "blobs": True
            }

            _, response, blobs = execute_query(self.client,
                                               [
                                                   QueryBuilder.find_command(
                                                       self.db_object, params=find_image_params),
                                                   QueryBuilder.find_command(
                                                       ObjectType.DESCRIPTOR, params=find_descriptor_params)
                                               ], [])

            find_descriptor_params = {
                "_ref": 1,
                "set": set_name,
                "k_neighbors": n_neighbors + 1,
                "blobs":     False,
                "distances": True,
                "uniqueids": True,
            }
            find_image_params = {
                "is_connected_to": {
                    "ref": 1,
                },
                "group_by_source": True,
                "results": {
                    "list": [self.img_id_prop]
                }
            }

            _, response, blobs = execute_query(self.client,
                                               [
                                                   QueryBuilder.find_command(
                                                       ObjectType.DESCRIPTOR, params=find_descriptor_params),
                                                   QueryBuilder.find_command(
                                                       self.db_object, params=find_image_params)
                                               ], blobs)

            try:
                descriptors = response[0]["FindDescriptor"]["entities"]
                ordered_descs_ids = [x["_uniqueid"] for x in descriptors]

                # Images are not sorted by distance, so we need to sort them
                # That's why we use "group_by_source":
                # To have a mapping between descriptors and associated images.
                cmd, = response[1].keys()
                imgs_map = response[1][cmd]["entities"]

                for desc_id in ordered_descs_ids:
                    img_info = imgs_map[desc_id][0]
                    # We could assert here that there is only one image per descriptor.
                    imgs_return.images_ids.append(img_info[self.img_id_prop])

            except Exception as e:
                print("Error with search: {}".format(response))
                print(e)
                print("Error with similarity search")

        return imgs_return

    def __draw_text_with_shadow(self, image, text, origin, color, typeface=cv2.FONT_HERSHEY_SIMPLEX, scale=0.75, thickness=2, shadow_color=None, shadow_radius=4, meta=None):
        width, height = image.shape[1], image.shape[0]
        thickness = max(1, int(min(width, height) / 200))
        if not shadow_color:
            shadow_color = self.__contrasting_color(color)

        scale = thickness / 3
        resolved_origin = np.array([origin])
        operations = self.query["operations"] if self.query and "operations" in self.query else [
        ]
        if meta and "adb_image_width" in meta and "adb_image_height" in meta:
            resolved_origin = resolve(resolved_origin, meta, operations)
        else:
            logger.warning(
                f"Cannot resolve text origin, missing image metadata in {meta}.")
        cv2.putText(image, text, resolved_origin[0], typeface, scale,
                    shadow_color, thickness + shadow_radius, cv2.LINE_AA)

        cv2.putText(image, text, resolved_origin[0], typeface,
                    scale, color, thickness, cv2.LINE_AA)

    def __draw_bbox(self, image, bbox, color, thickness=1):
        width, height = image.shape[1], image.shape[0]
        thickness = max(1, int(min(width, height) / 200))
        cv2.drawContours(image, [bbox], 0, color, thickness)

    def __get_color_for_tag(self, tag: str) -> Tuple[int, int, int]:
        if tag not in self.color_for_tag:
            color = self.__random_color()
            self.color_for_tag[tag] = color
        return self.color_for_tag[tag]

    def __draw_bbox_and_tag(self, image, bbox, tag, bound, meta, deferred_tags):
        color = self.__get_color_for_tag(tag)
        self.__draw_bbox(image, bbox, color)

        if tag:
            left = bound[0][0]
            top = bound[0][1]

            y = top - 15 if top - 15 > 15 else top + 15

            deferred_tags.append({
                "text": tag,
                "origin": (left, y),
                "color": color,
                "meta": meta
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
        width, height = image.shape[1], image.shape[0]
        thickness = max(1, int(min(width, height) / 200))

        for verts in polygon:
            np_verts = np.multiply(verts, 256)
            fill = image.copy()
            cv2.fillPoly(fill, [np_verts.astype(int)],
                         color, cv2.LINE_4, shift)
            cv2.addWeighted(fill, fill_alpha, image, 1 - fill_alpha, 0, image)
            cv2.polylines(image, [np_verts.astype(int)], True, color,
                          thickness, cv2.LINE_4, shift)

    def __draw_polygon_and_tag(self, image, polygon, tag, bounds, meta, deferred_tags):

        color = self.__get_color_for_tag(tag)

        self.__draw_polygon(image, polygon, color)

        if tag:
            left = bounds["x"]
            top = bounds["y"]
            right = bounds["x"] + bounds["width"]
            FONT_WIDTH = 10
            FONT_HEIGHT = 16

            y = top - FONT_HEIGHT
            x = (left + right - (FONT_WIDTH * len(tag))) // 2
            org = (max(x, 0), max(y, 2 * FONT_HEIGHT))

            deferred_tags.append({
                "text": tag,
                "origin": org,
                "color": color,
                "meta": meta
            })

    def display(self,
                show_bboxes: bool = False,
                bbox_constraints: Constraints = None,
                show_polygons: bool = False,
                limit: Union[int, object] = None,
                polygon_constraints: Constraints = None,
                polygon_tag_key: str = "_label",
                polygon_tag_format: str = "{}") -> None:
        """
        **Display images with annotations**

        Args:
            show_bboxes: bool, optional
                Show bounding boxes, by default False
            bbox_constraints: Constraints, optional
                Constraints for bounding boxes, by default None
            show_polygons: bool, optional
                Show polygons, by default False
            limit: Union[int, object], optional
                Number of images to display, by default None
            polygon_constraints: Constraints, optional
                Constraints for polygons, by default None
            polygon_tag_key: str, optional
                Key for the polygon tag, by default "_label"
            polygon_tag_format: str, optional
                Format for the polygon tag, by default "{}"
        """
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
            image = self.get_image_by_index(i)

            # Just decode the image from buffer
            nparr = np.frombuffer(image, dtype=np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            deferred_tags = []
            if show_bboxes:
                if not str(uniqueid) in self.images_bboxes:
                    self.__retrieve_bounding_boxes(i, bbox_constraints)

                bboxes = self.images_bboxes[uniqueid]["bboxes"]
                tags = self.images_bboxes[uniqueid]["tags"]
                meta = self.images_bboxes[uniqueid]["meta"]
                bounds = self.images_bboxes[uniqueid]["bounds"]

                # Draw a rectangle around the faces
                for bi in range(len(bboxes)):
                    self.__draw_bbox_and_tag(
                        image, bboxes[bi], tags[bi], bounds[bi], meta[bi], deferred_tags)

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
                meta = self.images_polygons[uniqueid]["meta"] if uniqueid in self.images_polygons else [
                ]

                for pi in range(len(polygons)):
                    self.__draw_polygon_and_tag(image, polygons[pi], tags[pi] if pi < len(
                        tags) else None, bounds[pi], meta[pi], deferred_tags)

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
                    image, tag["text"], tag["origin"], tag["color"], meta=tag["meta"] if "meta" in tag else None)

            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

            fig1, ax1 = plt.subplots()
            plt.imshow(image)

    def get_props_names(self) -> List[str]:
        """
        **Get the names of the properties that apply to images**

        Returns:
            properties (List[str]): The names of the properties of the images
        """
        dbutils = Utils.Utils(self.client)
        schema = dbutils.get_schema()

        try:
            dictio = schema["entities"]["classes"][self.db_object.value]["properties"]
            props_array = [key for key, val in dictio.items()]
        except:
            props_array = []
            print("Cannot retrieve properties")

        return props_array

    def get_properties(self, prop_list: Iterable[str] = []) -> Dict[str, Any]:
        """
        **Get the properties of the images**

        Args:
            prop_list (List[str], optional): The list of properties to retrieve. Defaults to [].

            Returns:
                property_values Dict[str, Any]: The properties of the images
        """
        if len(prop_list) == 0:
            return {}

        return_dictionary = {}

        try:
            for uniqueid in self.images_ids:

                find_image_params = {
                    "_ref": 1,
                    "constraints": {
                        self.img_id_prop: ["==", uniqueid]
                    },
                    "blobs": False,
                    "results": {
                        "list": prop_list
                    }
                }

                _, res, images = execute_query(self.client, [
                    QueryBuilder.find_command(self.db_object, params=find_image_params)], [])

                cmd, = res[0].keys()
                return_dictionary[str(
                    uniqueid)] = res[0][cmd]["entities"][0]
        except:
            print("Cannot retrieved properties")

        return return_dictionary


class Frames(Images):
    """
    **The python wrapper of frame images in ApertureDB.**

    Frames in ApertureDB are quite similar to images and so
    are modeled in python as a subclass.


    Args:
        client: The database connector, perhaps as returned by `CommonLibrary.create_connector`
    """
    db_object = ObjectType.FRAME

    def __init__(self, client, batch_size=100, response=None, **kwargs):
        super().__init__(client, batch_size=batch_size, response=response, **kwargs)
