from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)
ENTITY_CLASS = "EntityClass"
PROPERTIES = "properties"
CONSTRAINTS = "constraints"


class EntityDataCSV(CSVParser.CSVParser):
    """**ApertureDB Entity Data.**

    This class loads the Entity Data which is present in a CSV file,
    and converts it into a series of ApertureDB queries.

    :::note Is backed by a CSV file with the following columns:
    ``EntityClass``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``
    :::

    Example CSV file::

        EntityClass,name,lastname,age,id,constraint_id
        Person,John,Salchi,69,321423532,321423532
        Person,Johna,Salchi,63,42342522,42342522
        ...

    Example usage:

    ``` python

        data = EntityDataCSV("/path/to/EntityData.csv")
        loader = ParallelLoader(client)
        loader.ingest(data)
    ```


    :::info
    In the above example, the constraint_id ensures that a Entity with the specified
    id would be only inserted if it does not already exist in the database.
    :::

    """

    def __init__(self, filename: str, **kwargs):
        super().__init__(filename, **kwargs)

        self.props_keys = [x for x in self.header[1:]
                           if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.constraints_keys = [x for x in self.header[1:]
                                 if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
        self.command = "AddEntity"

    def get_indices(self):
        return {
            "entity": {
                cls: self.get_indexed_properties() for cls in self.df[ENTITY_CLASS].unique()
            }
        }

    def getitem(self, idx):
        idx = self.df.index.start + idx
        eclass = self.df.loc[idx, ENTITY_CLASS]
        q = []
        ae = self._basic_command(idx,
                                 custom_fields={
                                     "class": eclass
                                 })

        q.append(ae)
        return q, []

    def validate(self):
        if self.header[0] != ENTITY_CLASS:
            raise Exception("Error with CSV file field: " + ENTITY_CLASS)

# Used when a csv has a single entity type that needs to be deleted


class EntityDeleteDataCSV(CSVParser.CSVParser):
    """**ApertureDB Entity Delete Data.**

    This class loads the Entity Data which is present in a CSV file,
    and converts it into a series of ApertureDB deletes.

    :::note
    Expects a CSV file with the following columns:

        ``constraint_PROP1``
    :::

    Example CSV file::

        constraint_id
        321423532
        42342522
        ...

    Example usage:

   ```python

        data = ImageDeleteDataCSV("/path/to/UnusedImages.csv")
        loader = ParallelQuery(client)
        loader.query(data)
    ```


    :::info
    In the above example, the constraint_id ensures that a Entity with the specified
    id would be only deleted.

    Note that you can take a csv with normal prop data and this will ignore it, so you
    could use input to a loader to this.
    :::


    """

    def __init__(self, entity_class, filename, df=None, use_dask=False):
        super().__init__(filename, df=df, use_dask=use_dask)
        self.command = "Delete" + entity_class
        self.constraint_keyword = "constraints"
        if not use_dask:
            self.constraint_keys = [x for x in self.header[0:]]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        q = []
        entity_delete = self._basic_command(idx)

        q.append(entity_delete)
        return q, []

    def validate(self):
        # all we require is a valid csv with 1 or more columns.
        return True


class ImageDeleteDataCSV(EntityDeleteDataCSV):
    """
    **ApertureData CSV Loader class for deleting images**

    Usage details in EntityDeleteDataCSV
    """

    def __init__(self, filename, df=None, use_dask=False):
        super().__init__("Image", filename, df=df, use_dask=use_dask)
