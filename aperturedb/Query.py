from __future__ import annotations

from enum import Enum
from typing import List, Tuple

from aperturedb.Constraints import Constraints
from aperturedb.Operations import Operations
from aperturedb.Sort import Sort
from pydantic import BaseModel


class ObjectType(Enum):
    """
    Based on : [API description](/query_language/Overview/API%20Description)

    """
    BLOB = "_Blob"
    BOUNDING_BOX = "_BoundingBox"
    CONNECTION = "_Connection"
    DESCRIPTOR = "_Descriptor"
    DESCRIPTORSET = "_DescriptorSet"
    ENTITY = "_Entity"
    FRAME = "_Frame"
    IMAGE = "_Image"
    POLYGON = "_Polygon"
    VIDEO = "_Video"


class Config(Enum):
    SAVE_NAME = 1
    SAVE_VALUE = 2


config = Config.SAVE_NAME


def generate_save_query(
        obj: BaseModel,
        cached: List[str] = None,
        source_field: str = None,
        index: int = 1,
        parent: int = 0) -> Tuple(List[object], List[bytearray], str):
    """
    Takes the user model, and builds out a sequence of commands that creates
    a similar structure on apertureDB's graph.

    Args:
        obj (BaseModel): The object from the user domain.
        cached (List[str], optional): helps to optimize sending one blob per node. Defaults to None.
        source_field (str, optional): Preserves the relevant connection information from user objects. Defaults to None.
        index (int, optional): The index to start creating references from. Defaults to 1.
        parent (int, optional): The parent of the current node. Defaults to None.

    Returns:
        tuple : Contains 3 items:
            * List of commands with current node and its children
            * List of blobs with current node and its children
            * index, adjusting the count of children + self for the number of commands
              in current query
    """
    if cached == None:
        cached = []
    query = []
    dependents = []
    props = {}
    blobs = []
    cindex = index
    if obj.id not in cached:
        for p in obj.__dict__.keys():
            if "_" == p[0]:
                continue
            subobj = getattr(obj, p)
            if isinstance(subobj, int) or isinstance(subobj, str):
                props[p] = subobj
            elif isinstance(subobj, Enum):
                props[p] = subobj.name if config == Config.SAVE_NAME else subobj.value
            elif isinstance(subobj, list):
                for i, si in enumerate(subobj):
                    q, b, index = generate_save_query(
                        si, cached=cached, source_field=f"{p}[{i}]", index=index + 1, parent=cindex)
                    blobs.extend(b)
                    dependents.extend(q)
            else:
                q, b, index = generate_save_query(
                    subobj, cached=cached, source_field=f"{p}", index=index + 1, parent=cindex)
                dependents.extend(q)
                blobs.extend(b)

    # We still want to create dummy Add* commands so that references
    # for creating already created instances are mantained.
    params = {
        "_ref": cindex,
        "properties": props if "id" in props else {},
        "if_not_found": {
            "id": ["==", props["id"] if "id" in props else obj.id]
        }
    }
    if hasattr(obj, "type"):
        props.pop('type', None)
        if obj.type != ObjectType.ENTITY:
            props["user_type"] = type(obj).__name__
            # Generates a Add<ADB object> command
            if obj.id not in cached:
                query.append(
                    QueryBuilder.add_command(obj.type.value, params=params))
            else:
                params["constraints"] = params["if_not_found"]
                params.pop("if_not_found", None)
                params.pop("properties", None)
                query.append(
                    QueryBuilder.find_command(obj.type.value, params=params))
        if obj.type in [ObjectType.IMAGE, ObjectType.VIDEO, ObjectType.BLOB]:
            # Do not send blob, if Node has been added to set of commands.
            if obj.id not in cached:
                with open(obj.file, "rb") as instream:
                    blobs.append(instream.read())
                props.pop('file', None)
        else:
            # Generates an AddEntity
            query.append(
                QueryBuilder.add_command(type(obj).__name__, params=params))

        cached.append(obj.id)
    if parent > 0:
        params = {
            "src": parent,
            "dst": cindex,
            "class": type(obj).__name__,
            "properties": {
                "source_field": source_field
            }
        }
        query.append(
            QueryBuilder.add_command(
                ObjectType.CONNECTION.value, params=params)
        )
    query.extend(dependents)
    return query, blobs, index


class QueryBuilder():
    @classmethod
    def find_command(self, oclass: str, params: dict) -> dict:
        return self.build_command(oclass, params, "Find")

    @classmethod
    def add_command(self, oclass: str, params: dict) -> dict:
        return self.build_command(oclass, params, "Add")

    @classmethod
    def build_command(self, oclass, params, operation):
        command = {
            f"{operation}Entity": params
        }
        members = [m.value for m in ObjectType]
        if oclass.startswith("_"):
            if oclass in members:
                command = {
                    f"{operation}{oclass[1:]}": params
                }
            else:
                raise Exception(
                    f"Invalid Object type. Should not begin with _, except for {members}")
        else:
            if operation == "Find":
                params["with_class"] = oclass
            else:
                params["class"] = oclass
        return command


class Query():
    """
    This is the underlying class to generate a query using python code.
    """
    db_object = "Entity"
    next = None

    def connected_to(self,
                     spec: Query,
                     adj_to: int = 0) -> Query:
        """
        Can be used to connect two commands together, for non trivial queries.
        This is the lower level function to create a query.

        Args:
            spec (Query): One command of the query.
            adj_to (int, optional): The command to refer to. Defaults to 0.

        Returns:
            Query: The completed, connected query.
        """
        spec.adj_to = self.adj_to + 1 if adj_to == 0 else adj_to
        self.next = spec
        return self

    def command_properties(self, prop: str = "") -> List[str]:
        """
        Helper function to get the properties of all commands the query.

        Args:
            v (str, optional): _description_. Defaults to "".

        Returns:
            List[str]: Properties from commands in the order they should be executed.
        """
        chain = []
        p = self
        while p is not None:
            chain.append(getattr(p, prop))
            p = p.next
        return chain

    @classmethod
    def spec(cls,
             constraints: Constraints = None,
             operations: Operations = None,
             with_class: str = "",
             limit: int = -1,
             sort: Sort = None,
             list: List[str] = None,
             group_by_src: bool = False,
             blobs: bool = False
             ) -> Query:
        """
        The [specification](/query_language/Overview/API%20Description) for a command to be used in a query.

        Args:
            constraints (Constraints, optional): [Constraints](/query_language/Reference/shared_command_parameters/constraints) . Defaults to None.
            with_class (ObjectType, optional): _description_. Defaults to ObjectType.CUSTOM.
            limit (int, optional): _description_. Defaults to -1.
            sort (Sort, optional): _description_. Defaults to None.
            list (List[str], optional): _description_. Defaults to None.

        Returns:
            Query: The query object.
        """
        return Query(
            constraints=constraints,
            operations=operations,
            with_class=with_class,
            limit=limit,
            sort = sort,
            list = list,
            blobs=blobs,
            group_by_src = group_by_src
        )

    def __init__(self,
                 constraints: Constraints = None,
                 operations: Operations = None,
                 with_class: str = "",
                 limit: int = -1,
                 sort: Sort = None,
                 list: List[str] = None,
                 group_by_src: bool = False,
                 blobs: bool = False,
                 adj_to: int = 0):
        self.constraints = constraints
        self.operations = operations
        self.with_class = with_class
        self.limit = limit
        self.sort = sort
        self.list = list
        self.group_by_src = group_by_src
        self.blobs = blobs
        self.adj_to = adj_to + 1

    def query(self) -> List[dict]:
        results_section = "results"
        cmd_params = {results_section: {}}
        if self.limit != -1:
            cmd_params[results_section]["limit"] = self.limit
        if self.sort:
            cmd_params[results_section]["sort"] = self.sort._sort
        if self.list is not None and len(self.list) > 0:
            cmd_params[results_section]["list"] = self.list
        else:
            cmd_params[results_section]["all_properties"] = True
        cmd_params[results_section]["group_by_source"] = self.group_by_src

        if self.constraints:
            cmd_params["constraints"] = self.constraints.constraints
        if self.operations:
            cmd_params["operations"] = self.operations.operations_arr

        self.with_class = self.with_class if self.db_object == "Entity" else self.db_object
        cmd = QueryBuilder.find_command(
            oclass=self.with_class, params=cmd_params)
        self.find_command = list(cmd.keys())[0]
        query = [cmd]
        if self.next:
            next_commands = self.next.query()
            list(next_commands[0].values())[0]["is_connected_to"] = {
                "ref": self.adj_to
            }
            query.extend(next_commands)
        return query
