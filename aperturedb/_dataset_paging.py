"""Dataset paging selected by the negotiated server hello version."""
from copy import deepcopy

from aperturedb.ServerCompatibility import typed_server


class DatasetPaging:
    def __init__(self, client, query, command_idx, command_name, batch_size):
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.query = query
        self.command_idx = command_idx
        self.command_name = command_name
        self.batch_size = batch_size
        self.modern = typed_server(client)
        params = query[command_idx][command_name]
        self.offset = params.get("offset", 0)
        self.limit = params.get("limit")
        if self.modern:
            if any(not next(iter(command)).startswith(("Find", "Get")) for command in query):
                raise ValueError(
                    "Server 1.0 dataset paging requires a read-only query")
            if params.get("batch"):
                raise ValueError(
                    "Server 1.0 removed cached batch paging; use batch_size")
            params.pop("batch", None)
            if type(self.offset) is not int or self.offset < 0:
                raise ValueError(
                    "Dataset offset must be a nonnegative integer")
            if self.limit is not None and (type(self.limit) is not int or self.limit < 0):
                raise ValueError("Dataset limit must be a nonnegative integer")
        else:
            params["batch"] = {}

    def count_query(self):
        if not self.modern:
            return self.query
        query = deepcopy(self.query)
        params = query[self.command_idx][self.command_name]
        params.pop("offset", None)
        params.pop("limit", None)
        params["blobs"] = False
        params["results"] = {"count": True}
        return query

    def total(self, response):
        if not self.modern:
            return response.get("batch", {}).get("total_elements", response.get("returned", 0))
        total = max(0, response.get("count", 0) - self.offset)
        return min(total, self.limit) if self.limit is not None else total

    def page(self, batch_idx, total):
        params = self.query[self.command_idx][self.command_name]
        if self.modern:
            start = batch_idx * self.batch_size
            params["offset"] = self.offset + start
            params["limit"] = min(self.batch_size, total - start)
        else:
            params["batch"].update(
                batch_size=self.batch_size, batch_id=batch_idx)

    def sample_query(self):
        query = deepcopy(self.query)
        params = query[self.command_idx][self.command_name]
        params["blobs"] = False
        if self.modern:
            params["offset"] = self.offset
            params["limit"] = 1
        else:
            params.setdefault("batch", {}).update(batch_size=1, batch_id=0)
        return query
