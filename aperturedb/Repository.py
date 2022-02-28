from typing import List, Optional, Dict, ByteString
from aperturedb import Connector, Status, ObjectHelper
from collections import namedtuple

import types

FIND_COMMAND_PREFIX = "Find"
ADD_COMMAND_PREFIX = "Add"
UPDATE_COMMAND_PREFIX = "Update"
DELETE_COMMAND_PREFIX = "Delete"

all_objects = [
    "Blob",
    "BoundingBox",
    "Descriptor",
    "DescriptorSet",
    "Entity",
    "Image",
    "Video"
]

def connect_entity(src, dest):
    src._findquery["_ref"] = 1
    dest._findquery["_ref"] = 2
    connect_command = [
        src._findquery,
        dest._findquery,
        {
            "AddConnection": {
                "class": "edge",
                "src" : 1,
                "dest" : 2
            }
        }
    ]
    print(connect_command)
    resp, blob = src._db.query(connect_command)
    print(resp)


class Repository:
    def __init__(self, db: Connector.Connector) -> None:
        self._db = db
        self._status = Status.Status(db)
        self._schema = self._status.get_schema()
        self._object_type = "Entity"
        if self._object_type in self._schema["entities"]["classes"]:
            self._properties = [k for k in self._schema["entities"]["classes"][self._object_type]["properties"]]
        else:
            self._properties = {}
        
    def _find_command(self) -> str:
        return f"{FIND_COMMAND_PREFIX}{self._object_type}"

    def _add_command(self) -> str:
        return f"{ADD_COMMAND_PREFIX}{self._object_type}"

    def _create_object(self, resp):
        # Safe property names. Remove spaces.
        modified = {k.replace(" ", "_"): v for k,v in resp[0][self._find_command()]["entities"][0].items()}
        instance = ObjectHelper.dict_to_obj(modified, self._object_type)

        for o in all_objects:
            setattr(instance, f"connect_{o}", types.MethodType(connect_entity, instance))

        self._entity = instance
        return instance

    def save(self):
        """
        Persist an object in the Database and get its id.
        
        """
        create_query = {
            self._add_command() : {
                "properties": self._properties,
            }
        }
        if self._class is not None:
            create_query[self._add_command()]["class"] = self._class
        if self._label is not None:
            create_query[self._add_command()]["label"] = self._label
        if self._rectangle is not None:
            create_query[self._add_command()]["rectangle"] = self._rectangle


        if self._blob is not None:
            resp, blob = self._db.query([create_query], [self._blob])
        else:
            resp = self._db.query([create_query])
            
        retVal = None
        if 'status' not in resp:
            # TODO Change the VDMS to return the _uniqueid for all the Add operations.
            find_query = {
                self._find_command(): {
                    "constraints": {
                        p : ["==", self._properties[p]] for p in self._properties
                    },
                    "uniqueids": True,
                    "results" : {
                        "all_properties" : True,
                        "limit": 1
                    }
                }
            }
            
            resp, blob = self._db.query([find_query])
            retVal = self._create_object(resp)
        else:
            Error = namedtuple('Error', [k for k in resp])
            e = Error(**resp)
            retVal = e

        return retVal

    def get(self, id):
        """
        Get a unique Object from the database by it's unique property.
        """
        find_query = {
                self._find_command(): {
                    "constraints": {
                        "_uniqueid": ["==", id]
                    },
                    "results" : {
                        "all_properties" : True
                    }
                }
        }
        resp, blob = self._db.query([find_query])
        if resp[0][self._find_command()]["returned"] != 1:
            raise Exception("not found unique entity")
        entity = self._create_object(resp)
        setattr(entity, "_findquery", find_query)
        setattr(entity, "_db", self._db)
        return entity

    def create_new(self, 
        properties: Optional[Dict] = {}, 
        operations: Optional[List[Dict]] = [], 
        blob: Optional[ByteString] = None,
        rectangle:Optional[Dict] = None,
        label: Optional[str] = None,
        eclass:str=None):
        """
        A Create new function discards all previous information
        """
        self._properties = properties
        self._blob = blob
        self._operations = operations
        self._class = eclass
        self._rectangle = rectangle
        self._label = label