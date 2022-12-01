from __future__ import annotations

from enum import Enum
from typing import List

from aperturedb.Constraints import Constraints
from aperturedb.Sort import Sort


class ObjectType(Enum):
    """
    Based on : https://docs.aperturedata.io/API-Description.html

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


class QueryBuilder():
    @classmethod
    def find_command(self, oclass: str, params: dict) -> dict:
        command = {
            "FindEntity": params
        }
        members = [m.value for m in ObjectType]
        if oclass.startswith("_"):
            if oclass in members:
                command = {
                    f"Find{oclass[1:]}": params
                }
            else:
                raise Exception(
                    f"Invalid Object type. Should not begin with _, exceept for {members}")
        else:
            params["with_class"] = oclass
        return command


class Query():
    """
    This is the underlying class to generate a query using python code.
    """
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"
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
             with_class: str = "",
             limit: int = -1,
             sort: Sort = None,
             list: List[str] = None,
             group_by_src: bool = False,
             blobs: bool = False
             ) -> Query:
        """
        The specification for a command to be used in a query.
        https://docs.aperturedata.io/API-Description.html?highlight=query

        Args:
            constraints (Constraints, optional): https://docs.aperturedata.io/parameters/constraints.html . Defaults to None.
            with_class (ObjectType, optional): _description_. Defaults to ObjectType.CUSTOM.
            limit (int, optional): _description_. Defaults to -1.
            sort (Sort, optional): _description_. Defaults to None.
            list (List[str], optional): _description_. Defaults to None.

        Returns:
            Query: The query object.
        """
        return Query(
            constraints=constraints,
            with_class=with_class,
            limit=limit,
            sort = sort,
            list = list,
            blobs=blobs,
            group_by_src = group_by_src
        )

    def __init__(self,
                 constraints: Constraints = None,
                 with_class: str = "",
                 limit: int = -1,
                 sort: Sort = None,
                 list: List[str] = None,
                 group_by_src: bool = False,
                 blobs: bool = False,
                 adj_to: int = 0):
        self.constraints = constraints
        self.with_class = with_class
        self.limit = limit
        self.sort = sort
        self.list = list
        self.group_by_src = group_by_src
        self.blobs = blobs
        self.adj_to = adj_to + 1

    def query(self) -> List[dict]:
        results_section = "results"
        cmd = {
            self.find_command: {
                "_ref": self.adj_to,
                results_section: {

                }
            }
        }
        if self.db_object == "Entity":
            cmd[self.find_command]["with_class"] = self.with_class
        if self.limit != -1:
            cmd[self.find_command][results_section]["limit"] = self.limit
        if self.sort:
            cmd[self.find_command][results_section]["sort"] = self.sort._sort
        if self.list is not None and len(self.list) > 0:
            cmd[self.find_command][results_section]["list"] = self.list
        else:
            cmd[self.find_command][results_section]["all_properties"] = True
        cmd[self.find_command][results_section]["group_by_source"] = self.group_by_src

        if self.constraints:
            cmd[self.find_command]["constraints"] = self.constraints.constraints

        query = [cmd]
        if self.next:
            next_commands = self.next.query()
            next_commands[0][self.next.find_command]["is_connected_to"] = {
                "ref": self.adj_to
            }
            query.extend(next_commands)
        return query
