from aperturedb import CSVParser
import logging

logger = logging.getLogger(__name__)
ENTITY_CLASS = "EntityClass"
PROPERTIES   = "properties"
CONSTRAINTS  = "constraints"


class EntityDataCSV(CSVParser.CSVParser):
    """**ApertureDB Entity Data.**

    This class loads the Entity Data which is present in a csv file,
    and converts it into a series of aperturedb queries.

    .. note::
        Expects a csv file with the following columns:

            ``EntityClass``, ``PROP_NAME_1``, ... ``PROP_NAME_N``, ``constraint_PROP1``

    Example csv file::

        EntityClass,name,lastname,age,id,constraint_id
        Person,John,Salchi,69,321423532,321423532
        Person,Johna,Salchi,63,42342522,42342522
        ...

    Example usage:

    .. code-block:: python

        data = EntityDataCSV("/path/to/EntityData.csv")
        loader = ParallelLoader(db)
        loader.ingest(data)


    .. important::
        In the above example, the constraint_id ensures that a Entity with the specified
        id would be only inserted if it does not already exist in the database.


    """

    def __init__(self, filename, df=None, use_dask=False):
        super().__init__(filename, df=df, use_dask=use_dask)
        if not use_dask:
            self.props_keys       = [x for x in self.header[1:]
                                     if not x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.constraints_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
            self.command = "AddEntity"
            self.search_keys = [x for x in self.header[1:]
                                     if x.startswith(CSVParser.SEARCH_PREFIX)]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        eclass = self.df.loc[idx, ENTITY_CLASS]
        q = []
        ae = self._basic_command(idx,
                                 custom_fields = {
                                     "class": eclass
                                 })

        q.append(ae)
        return q, []

    def validate(self):
        if self.header[0] != ENTITY_CLASS:
            raise Exception("Error with CSV file field: " + ENTITY_CLASS)

# Used when a csv has a single entity type that needs to be deleted
class SingleEntityDeleteCSV(CSVParser.CSVParser):
    def __init__(self, entity_class, filename, df=None, use_dask=False):
        super().__init__(filename, df=df, use_dask=use_dask)
        self.command = "Delete" + entity_class
        self.constraint_keyword = "constraints"
        if not use_dask:
            self.constraint_keys       = [x for x in self.header[0:]]

    def getitem(self, idx):
        idx = self.df.index.start + idx
        q = []
        entity_delete = self._basic_command(idx)

        q.append(entity_delete)
        return q, []
    def validate(self):
        # all we require is a valid csv with 1 or more columns.
        return True

class ImageDeleteCSV(SingleEntityDeleteCSV):
    def __init__(self, filename, df=None, use_dask=False):
        super().__init__("Image",filename, df=df, use_dask=use_dask)

# we need to update conditionally; if_not_found only works for add.
# additionally there are two metrics in use: selection critera
# and update critera.
# find_<prop> is the column form to bind 
class SingleEntityUpdateCSV(CSVParser.CSVParser):
    def __init__(self, entity_class, filename, df=None, use_dask=False):
        self.entity = entity_class
        self.command = "Update" + entity_class
        self.findcommand = "Find" + entity_class
        self.keys_set = False
        super().__init__(filename, df=df, use_dask=use_dask)
        self.blobs_per_query = [0,0]
        self.commands_per_query = [1,1]
        self._setupkeys()

    def _setupkeys(self):
        if not self.keys_set:
            if not self.use_dask:
                print("Setting CS")
                self.keys_set = True
                self.props_keys       = [x for x in self.header[1:]
                                         if not ( x.startswith(CSVParser.CONSTRAINTS_PREFIX)
                                             or x.startswith(CSVParser.SEARCH_PREFIX) ) ]
                self.constraints_keys       = [x for x in self.header[1:]
                                            if x.startswith(CSVParser.CONSTRAINTS_PREFIX)]
                self.search_keys       = [x for x in self.header[1:]
                                            if x.startswith(CSVParser.SEARCH_PREFIX)]
    def getitem(self, idx):
        idx = self.df.index.start + idx
        query_set = []

        self.constraint_keyword = "if_not_found"
        self.command = "Add" + self.entity
        entity_add = self._basic_command(idx)
        condition_add_failed = { "results": { 0: { "status" : [ "==", 2 ] } } }
        self.command = "Update" + self.entity
        update_constraints = self.parse_constraints(self.df,idx)
        search_constraints = self.parse_search(self.df, idx)
        update_constraints.update(search_constraints)
        properties = self.parse_properties(self.df, idx)
        print("UC = " ,update_constraints)
        print("SC = ", search_constraints)
        self.constraint_keyword = "constraints"
        entity_update = self._parsed_command(idx,None,update_constraints,properties)
        print("ADD CMD = ",entity_add)
        print("UPDATE CMD = ",entity_update)
        query_set.append(entity_add)
        query_set.append([condition_add_failed,entity_update])



        if hasattr(self, "modify_item") and callable(self.modify_item):
            query_set = self.modify_item(query_set,idx)
            

        return [query_set], []
    def validate(self):
        self._setupkeys()
        valid = True
        if not self.use_dask:
            print("Testing CS")
            if len(self.constraints_keys) > 0:
                logger.error("Cannot add/update " + self.entity + "; no constraint keys")
                valid = False
            if valid and len(self.update_constraint_keys) > 0:
                logger.error("Cannot update " + self.entity + "; no update constraint keys")
                valid = False
        return valid


class EntityUpdateCSV(SingleEntityUpdateCSV):
    def __init__(self, entity_type, filename, df=None, use_dask=False):
        super().__init__( "Entity", filename,df,use_dask)
        self.entity_type = entity_type
    def modify_item(self,query_set,idx):
        query_set[0]["AddEntity"]["class"] = self.entity_type
        return query_set
# Update and Add Images
class ImageUpdateCSV(SingleEntityUpdateCSV):
    def __init__(self, filename, df=None, use_dask=False):
        super().__init__( "Image", filename,df,use_dask)

