import json

from aperturedb import CSVParser

HEADER_POLYGONS = "polygons"
IMG_KEY_PROP = "img_key_prop"
IMG_KEY_VAL = "img_key_value"
POLYGON_FIELDS = {
    "_label": "label",
}


class PolygonDataCSV(CSVParser.CSVParser):
    """
    **ApertureDB Polygon Data.**

    This class loads the Polygon Data which is present in a CSV file,
    and converts it into a series of ApertureDB queries.

    :::note Is backed by a CSV file with the following columns:
    ``IMG_KEY``, [``POLYGON_PROPERTY_1``, ... ``POLYGON_PROPERTY_N``,] [``constraint_POLYGON_PROPERTY_1``, ... ``constraint_POLYGON_PROPERTY_N``,] [``_label``,] ``polygons``
    :::

    **IMG_KEY**: identifies the name of the image property that will identify the
    image with which to associate each polygon object. This property should reliably
    identify at most a single image, like a unique id. The value in each row will be
    used to look up the image to which the polygon will attach.

    **POLYGON_PROPERTY_I**: declares the name of a property that will be assigned to all polygon objects. Any number of properties can be declared in this way.

    **constraint_POLYGON_PROPERTY_I**: declares that POLYGON_PROPERTY_I should be unique, and that a new polygon will not be added if there already exists one with the same value for this property. For each row, the value in this column should match the value in column POLYGON_PROPERTY_I.

    **_label**: optionally applies a label to the polygon objects.

    **polygons**: a JSON array of polygon regions. Each polygon region is itself an array of [x,y] vertices that describe the boundary of a single contiguous polygon. See also [Polygon API parameter](/query_language/Reference/shared_command_parameters/polygons).

    Example CSV file::

        image_id,polygon_id,constraint_polygon_id,category_id,_label,polygons
        397133,82445,82445,44,bottle,"[[[224.24, 297.18], [228.29, 297.18], ...]]"
        397133,119568,119568,67,dining table,"[[[292.37, 425.1], [340.6, 373.86], ...]]"
        ...

    Example usage:

    ``` python

        data = PolygonDataCSV("/path/to/PolygonData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```

    """

    def __init__(self, filename: str, **kwargs):

        super().__init__(filename, kwargs=kwargs)

        self.props_keys = []
        self.constraints_keys = []
        self.polygon_keys = []
        for key in self.header[1:-1]:
            if key in POLYGON_FIELDS.keys():
                self.polygon_keys.append(key)
            elif key.startswith(CSVParser.CONSTRAINTS_PREFIX):
                self.constraints_keys.append(key)
            else:
                self.props_keys.append(key)

        self.img_key = self.header[0]
        self.command = "AddPolygon"

    def get_indices(self):
        return {
            "entity": {
                "_Polygon": self.get_indexed_properties()
            }
        }

    def getitem(self, idx):
        idx = self.df.index.start + idx

        q = []

        img_id = self.df.loc[idx, self.img_key]

        fi = {
            "FindImage": {
                "_ref": 1,
                "constraints": {
                    self.img_key: ["==", img_id],
                },
                "blobs": False,
            },
        }
        q.append(fi)

        polygon_fields = {
            "image_ref": 1,
            "polygons": json.loads(self.df.loc[idx, HEADER_POLYGONS])
        }
        for key in self.polygon_keys:
            polygon_fields[POLYGON_FIELDS[key]] = self.df.loc[idx, key]

        ap = self._basic_command(idx, polygon_fields)
        q.append(ap)

        return q, []

    def validate(self):

        self.header = list(self.df.columns.values)

        if len(self.header) < 2:
            raise Exception(
                "Error with CSV file: must have at least two columns")
        if self.header[-1] != HEADER_POLYGONS:
            raise Exception("Error with CSV file field: " + HEADER_POLYGONS)
