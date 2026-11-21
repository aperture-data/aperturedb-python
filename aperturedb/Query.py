from __future__ import annotations

from enum import Enum
from typing import List, Tuple, Union

from aperturedb.Constraints import Constraints, predicate
from aperturedb.Operations import Operations
from aperturedb.Sort import Sort
from pydantic import BaseModel
from aperturedb.Sources import Sources
from aperturedb.ServerCompatibility import OBJECT_TYPES, hello_version as negotiated_hello_version, vector_index_commands
from aperturedb.DescriptorEngines import engines_object, descriptor_search
import struct


class ObjectType(Enum):
    """
    Based on : [API description](/query_language/Overview/API%20Description)

    """
    BLOB = "_Blob"
    BOUNDING_BOX = "_BoundingBox"
    CONNECTION = "_Connection"
    CLIP = "_Clip"
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


class PropertyType(str, Enum):
    SYSTEM = "system"
    USER = "user"


class RangeType(str, Enum):
    TIME = "time"
    FRAME = "frame"
    FRACTION = "fraction"


config = Config.SAVE_NAME


def class_entity(db_class: Union[str, ObjectType]):
    members = [m.value for m in ObjectType]
    db_class = db_class if not isinstance(
        db_class, ObjectType) else db_class.value
    if ":" in db_class:
        target = db_class.split(":", 1)[0]
        if target in OBJECT_TYPES.values():
            return next(name for name, value in OBJECT_TYPES.items() if value == target)
    if db_class.startswith("_"):
        if db_class in members:
            return f"{db_class[1:]}"
        else:
            raise Exception(
                f"Invalid Object type. Should not begin with _, except for {members}")
    else:
        return "Entity"


def get_handlers():
    sources = Sources(n_download_retries=3)
    source_url_handlers = {
        "http": sources.load_from_http_url,
        "https": sources.load_from_http_url,
        "": sources.load_from_file,
        "s3": sources.load_from_s3_url,
        "gs": sources.load_from_gs_url
    }
    return source_url_handlers


def get_specific(obj: BaseModel, hello_version: int = 1) -> dict:
    """This function is used to get the specific parameters for the object.
    Example: For a Descriptor object, the vector is packed into a blob.
    """
    if obj.type == ObjectType.CLIP:
        range_types = {
            RangeType.TIME: "time_offset_range",
            RangeType.FRAME: "frame_number_range",
            RangeType.FRACTION: "time_fraction_range"
        }
        start, stop = obj.start, obj.stop
        if obj.range_type == RangeType.TIME:
            start, stop = int(start), int(stop)
            start = "{:0>2}:{:0>2}:{:0>2}".format(
                start // 3600, (start // 60) % 60, start % 60)
            stop = "{:0>2}:{:0>2}:{:0>2}".format(
                stop // 3600, (stop // 60) % 60, stop % 60)
        elif obj.range_type == RangeType.FRAME:
            start = int(obj.start)
            stop = int(obj.stop)
        return {
            range_types[obj.range_type]: {
                "start": start,
                "stop": stop,
            }
        }, []
    elif obj.type == ObjectType.DESCRIPTOR:
        blob = struct.pack("%sf" % len(obj.vector), *obj.vector)
        return {"class" if hello_version >= 2 else "set": obj.set.name}, [blob]
    elif obj.type == ObjectType.DESCRIPTORSET:
        return {
            "name": obj.name,
            "dimensions": obj.dimensions
        }, []
    return {}, []


def generate_add_query(
        obj: BaseModel,
        cached: List[str] = None,
        source_field: str = None,
        index: int = 1,
        parent: int = 0,
        client=None,
        hello_version: int = None) -> Tuple[List[object], List[bytearray], int]:
    """
    Takes the user model, and builds out a sequence of commands that creates
    a similar structure on ApertureDB's graph. For example, given the following

    ```mermaid
    erDiagram
          VIDEO {
            CLIP range
          }
          CLIP{
            DESCRIPTOR vector
          }
          VIDEO ||--o{ CLIP : clips
          CLIP ||--|| DESCRIPTOR : descriptor
    ```

    The function will generate the following commands:

    *AddVideo 1*
        *AddClip 2*
        *AddDescriptor 3*
        *AddConnection 3-2*

    .
    .
    .

    Args:
        obj (BaseModel): The object from the user domain.
        cached (List[str], optional): helps to optimize sending one blob per node. Defaults to None.
        source_field (str, optional): Preserves the relevant connection information from user objects. Defaults to None.
        index (int, optional): The index to start creating references from. Defaults to 1.
        parent (int, optional): The parent of the current node. Defaults to None.
        client (Connector, optional): The connected client used to select the server's command API.

    Returns:
        tuple : Contains 3 items:
            * List of commands with current node and its children
            * List of blobs with current node and its children
            * index, adjusting the count of children + self for the number of commands
              in current query
    """
    source_url_handlers = get_handlers()
    version = negotiated_hello_version(
        client) if hello_version is None else hello_version
    if version not in (1, 2):
        raise ValueError("Model queries support hello versions 1 and 2")
    if cached == None:
        cached = []
    query = []
    dependents = []
    dependents_blobs = []
    props = {}
    blobs = []
    cindex = index
    specific_params = {}
    specific_blobs = []
    if obj is None:
        return query, blobs, index
    if obj.id not in cached:
        for p in obj.__dict__.keys():
            if "_" == p[0]:
                continue
            subobj = getattr(obj, p)
            meta = type(obj).model_fields[p].metadata
            if len(meta) > 0 and meta[0] == PropertyType.SYSTEM:
                specific_params, specific_blobs = get_specific(obj, version)
            elif isinstance(subobj, int) or isinstance(subobj, str) or isinstance(subobj, str) or isinstance(subobj, float):
                props[p] = subobj
            elif isinstance(subobj, Enum):
                props[p] = subobj.name if config == Config.SAVE_NAME else subobj.value
            elif "vector" == p:
                pass
            elif isinstance(subobj, list):
                for i, si in enumerate(subobj):
                    q, b, index = generate_add_query(
                        si, cached=cached, source_field=f"{p}[{i}]", index=index + 1, parent=cindex,
                        client=client, hello_version=version)
                    dependents_blobs.extend(b)
                    dependents.extend(q)
            else:
                q, b, index = generate_add_query(
                    subobj, cached=cached, source_field=f"{p}", index=index + 1, parent=cindex,
                    client=client, hello_version=version)
                dependents.extend(q)
                dependents_blobs.extend(b)

    if version >= 2 and obj.type == ObjectType.DESCRIPTORSET:
        if parent or dependents:
            raise ValueError(
                "Server 1.0 vector indexes cannot be graph objects or connection endpoints")
        return vector_index_commands(version, obj.name, obj.dimensions,
                                     getattr(obj, "metric", None) or ["CS"],
                                     getattr(obj, "engines", None) or engines_object("IVF"), props), [], index

    # We still want to create dummy Add* commands so that references
    # for creating already created instances are mantained.
    params = {
        "_ref": cindex,
        "properties": props if "id" in props else {},
    }
    if obj.type != ObjectType.DESCRIPTORSET:
        params["if_not_found"] = predicate(
            "id", "==", props["id"] if "id" in props else obj.id)
    for k, v in specific_params.items():
        params[k] = v
    if obj.type == ObjectType.DESCRIPTOR:
        params["class" if version >= 2 else "set"] = obj.set.name
    if version >= 2 and obj.type != ObjectType.ENTITY:
        class_name = params.get("class", getattr(
            obj, "class_name", None) or type(obj).__name__)
        if not isinstance(class_name, str) or not class_name or class_name.startswith("_"):
            raise ValueError("Model objects require an application class_name")
        params["class"] = class_name
    blobs.extend(specific_blobs)
    if hasattr(obj, "type"):
        props.pop('type', None)
        if obj.type != ObjectType.ENTITY:
            props["user_type"] = type(obj).__name__
            # Generates a Add<ADB object> command
            if obj.id not in cached:
                query.append(
                    QueryBuilder.add_command(obj.type.value, params=params))
            else:
                if "if_not_found" in params:
                    params["constraints"] = params["if_not_found"]
                    params.pop("if_not_found", None)
                params.pop("properties", None)
                if version >= 2:
                    params["with_class"] = params.pop("class")
                query.append(
                    QueryBuilder.find_command(obj.type.value, params=params))
        if obj.type in [ObjectType.IMAGE, ObjectType.VIDEO, ObjectType.BLOB]:
            # Do not send blob, if Node has been added to set of commands.
            if obj.id not in cached:
                if obj.url:
                    parts = obj.url.split(":")
                    if len(parts) > 1:
                        _, blob = source_url_handlers[parts[0]](
                            obj.url, lambda x: True)
                        blobs.append(blob)
                    else:
                        _, blob = source_url_handlers[""](obj.url)
                        blobs.append(blob)
                props.pop('url', None)
        else:
            # Generates an AddEntity
            if obj.type == ObjectType.ENTITY:
                query.append(
                    QueryBuilder.add_command(
                        (getattr(obj, "class_name", None) or type(obj).__name__) if version >= 2 else type(obj).__name__, params=params))

        cached.append(obj.id)

    if parent > 0:
        if obj.type == ObjectType.CLIP:
            if "AddClip" in query[0]:
                query[0]["AddClip"]["video_ref"] = parent

        else:
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
    blobs.extend(dependents_blobs)
    return query, blobs, index


class QueryBuilder():
    @staticmethod
    def split_type_selector(selector):
        """Split explicit selectors such as ``image:Assets`` into type/class."""
        if not isinstance(selector, str) or ":" not in selector:
            return None, None
        target, class_name = selector.split(":", 1)
        targets = set(OBJECT_TYPES.values())
        if target not in targets:
            return None, None
        if not class_name or class_name.startswith("_"):
            raise ValueError(
                "Typed selectors require a nonempty application class")
        return target, class_name

    @classmethod
    def get_query_cmd(cls, query: Union[dict, list]) -> str:
        known_constraint_keywords = ['results', 'apply']
        if isinstance(query, dict):
            if len(query) != 1:
                raise ValueError("Command dict must have exactly 1 key")
            return list(query.keys())[0]
        elif isinstance(query, list) and len(query) == 2 and isinstance(query[0], dict) \
                and query[0] and all(k in known_constraint_keywords for k in query[0].keys()) \
                and isinstance(query[1], dict):
            if len(query[1]) != 1:
                raise ValueError(
                    "Command dict in query list must have exactly 1 key")
            return list(query[1].keys())[0]
        else:
            q_type = type(query).__name__
            q_len = len(query) if hasattr(query, '__len__') else 'N/A'
            q_keys = list(query.keys()) if isinstance(query, dict) else 'N/A'
            raise ValueError(
                f"Cannot extract command from query structure. Expected single-command dict or [constraints_dict, command_dict]. Received: type={q_type}, len={q_len}, keys={q_keys}")

    @classmethod
    def find_command(self, oclass: Union[str, ObjectType], params: dict) -> dict:
        return self.build_command(oclass, params, "Find")

    @classmethod
    def add_command(self, oclass: Union[str, ObjectType], params: dict) -> dict:
        return self.build_command(oclass, params, "Add")

    @classmethod
    def build_command(self, oclass: Union[str, ObjectType], params: dict, operation: str) -> dict:
        oclass = oclass if not isinstance(oclass, ObjectType) else oclass.value
        target, class_name = self.split_type_selector(oclass)
        if target == "connection":
            classes = params.get("class", {})
            if "with_class" in params or not isinstance(classes, dict) or "connection" in classes:
                raise ValueError(
                    "The typed selector already supplies the connection class")
            if operation == "Add" and ("class" in params or "target" in params):
                raise ValueError(
                    "AddConnection determines endpoint classes and types from its references")
            if operation == "Find":
                types = params.get("target")
                for side in ("src", "dst"):
                    class_filter = side in classes
                    type_filter = isinstance(types, dict) and side in types
                    if side in params and (class_filter or type_filter):
                        raise ValueError(
                            f"The {side} reference already identifies that endpoint")
            if "class" in params:
                grouped = {**classes, "connection": class_name}
                return {f"{operation}Connection": {**params, "class": grouped}}
            params = dict(params)
        if target is not None:
            object_type = next(
                name for name, value in OBJECT_TYPES.items() if value == target)
            params["class" if operation ==
                   "Add" else "with_class"] = class_name
            return {f"{operation}{object_type}": params}
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
            if operation != "Add":
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
            prop (str): Name of the query attribute to retrieve from each command.

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
             blobs: bool = False,
             set: str = None,
             vector: List[float] = None,
             k_neighbors: int = 0,
             engine: str = None,
             metric: str = None
             ) -> Query:
        """
        The [specification](/query_language/Overview/API%20Description) for a command to be used in a query.

        Args:
            constraints (Constraints, optional): [Constraints](/query_language/Reference/shared_command_parameters/constraints) . Defaults to None.
            with_class (ObjectType, optional): _description_. Defaults to ObjectType.CUSTOM.
            limit (int, optional): _description_. Defaults to -1.
            sort (Sort, optional): _description_. Defaults to None.
            list (List[str], optional): _description_. Defaults to None.
            engine (str, optional): Descriptor search engine for k_neighbors searches.
                Defaults to the preferred index on the selected descriptor class.
            metric (str, optional): Descriptor distance metric. When a client is supplied,
                the first configured metric is selected by default.

        Returns:
            Query: The query object.
        """
        return Query(
            constraints=constraints,
            operations=operations,
            with_class=with_class,
            limit=limit,
            sort=sort,
            list=list,
            blobs=blobs,
            group_by_src=group_by_src,
            set=set,
            vector=vector,
            k_neighbors=k_neighbors,
            engine=engine,
            metric=metric
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
                 adj_to: int = 0,
                 set: str = None,
                 vector: List[float] = None,
                 k_neighbors: int = 0,
                 engine: str = None,
                 metric: str = None
                 ):
        self.constraints = constraints
        self.operations = operations
        self.with_class = with_class
        self.limit = limit
        self.sort = sort
        self.list = list
        self.group_by_src = group_by_src
        self.blobs = blobs
        self.adj_to = adj_to + 1
        self.set = set
        self.vector = vector
        self.k_neighbors = k_neighbors
        self.engine = engine
        self.metric = metric

    def query(self, client=None, hello_version: int = None) -> List[dict]:
        version = negotiated_hello_version(
            client) if hello_version is None else hello_version
        if version not in (1, 2):
            raise ValueError("Query builders support hello versions 1 and 2")
        object_type = self.db_object.value if isinstance(
            self.db_object, ObjectType) else self.db_object
        if version >= 2 and self.with_class:
            target, class_name = QueryBuilder.split_type_selector(
                self.with_class)
            if target is not None and object_type != "Entity":
                if OBJECT_TYPES.get(object_type.lstrip("_")) != target:
                    raise ValueError(
                        "Typed class selector conflicts with the query's object type")
                self.with_class = class_name
            selected = class_name if target == "descriptor" else self.with_class
            if self.set and selected != self.set and (target == "descriptor" or object_type == ObjectType.DESCRIPTOR.value):
                raise ValueError(
                    "set and with_class select different descriptor classes")
        results_section = "results"
        cmd_params = {results_section: {}, "_ref": self.adj_to}
        if self.limit != -1:
            cmd_params["limit"] = self.limit
        if self.sort:
            cmd_params["sort"] = self.sort._sort
        if self.list is not None and len(self.list) > 0:
            cmd_params[results_section]["list"] = self.list
        else:
            cmd_params[results_section]["all_properties"] = True
        cmd_params["group_by_source"] = self.group_by_src

        if self.constraints and self.constraints.constraints:
            cmd_params["constraints"] = self.constraints.constraints
        if self.operations:
            cmd_params["operations"] = self.operations.operations_arr
        if self.set:
            cmd_params["with_class" if version >= 2 else "set"] = self.set
        if self.k_neighbors > 0:
            cmd_params["k_neighbors"] = self.k_neighbors
            if self.engine:
                cmd_params["engine"] = self.engine
            if self.metric:
                cmd_params["metric"] = self.metric
            if version >= 2:
                descriptor_class = self.set or self.with_class
                if not descriptor_class:
                    raise ValueError(
                        "A descriptor class is required for nearest-neighbor search")
                if client is None:
                    if not self.engine or not self.metric:
                        raise ValueError(
                            "Pass client= to resolve vector-index engine and metric")
                else:
                    target, name = QueryBuilder.split_type_selector(
                        descriptor_class)
                    cmd_params.update(descriptor_search(client, name if target == "descriptor" else descriptor_class,
                                                        self.engine, self.metric))

        self.blob = None
        if self.vector:
            self.blob = struct.pack("%sf" % len(self.vector), *self.vector)

        object_type = self.db_object.value if isinstance(
            self.db_object, ObjectType) else self.db_object
        if version < 2:
            self.with_class = self.with_class if object_type == "Entity" else object_type
        elif object_type != "Entity" and self.with_class:
            cmd_params["with_class"] = self.with_class
        selector = self.with_class if object_type == "Entity" else object_type
        target, _ = QueryBuilder.split_type_selector(selector)
        self.registry_type = "_" + \
            next(name for name, value in OBJECT_TYPES.items()
                 if value == target) if target else selector
        if version >= 2 and object_type == ObjectType.DESCRIPTORSET.value:
            raise ValueError(
                "Server 1.0 descriptor sets are vector indexes; use Utils.get_indexes()")
        cmd = QueryBuilder.find_command(
            oclass=selector, params=cmd_params)
        self.find_command = list(cmd.keys())[0]
        query = [cmd]
        blobs = []
        if self.blob:
            blobs.append(self.blob)

        if self.next:
            next_commands, next_blobs = self.next.query(
                client=client, hello_version=version)
            list(next_commands[0].values())[0]["is_connected_to"] = {
                "ref": self.adj_to
            }
            # list(next_commands[0].values())[0]["_ref"] = self.adj_to
            query.extend(next_commands)
            blobs.extend(next_blobs)

        return query, blobs
