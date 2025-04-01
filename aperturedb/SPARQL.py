# SPARQL wrapper for ApertureDB
#
# This script allows ApertureDB to be queried using SPARQL queries.
# It is based on the rdflib library.
# Currently, it supports a subset of BGP queries, but it can be extended to support more complex queries.

from typing import Dict, Generator, List, Tuple, Union, Optional
from urllib.parse import quote, unquote
import json
import math
import logging

from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb.Utils import Utils


class SPARQL:
    def __init__(self, client=None, debug=False, log_level=None):
        """SPARQL compatability layer for ApertureDB

        Args:
            client: ApertureDB client. If not supplied, then `create_connector()` is used to create a new client.
            debug: bool. If True, then certain intermediate results are stored in the object.
            log_level: int. The logging level to use for the ApertureDB client.
        """
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if log_level is not None:
            self.logger.setLevel(log_level)

        try:
            import rdflib
        except ImportError:
            raise ImportError(
                "The rdflib library is required to use the SPARQL interface. "
                "Please install it using 'pip install rdflib'."
            )

        if client is None:
            client = create_connector()
        self._client = client
        self._utils = Utils(client)
        namespace = rdflib.Namespace("http://aperturedb.io/")
        self.namespaces = {
            "t": namespace["type/"],
            "c": namespace["connection/"],
            "p": namespace["property/"],
            "o": namespace["object/"],
            "knn": namespace["knn/"],
        }
        self._load_schema()
        # TODO: Only one instance of SPARQL can be used at a time
        import rdflib.plugins.sparql
        rdflib.plugins.sparql.CUSTOM_EVALS["aperturedb"] = self.eval
        self.graph = rdflib.Graph()
        for k, v in self.namespaces.items():
            self.graph.bind(k, v)
        self.knn_properties = set(self.namespaces["knn"] + p for p in [
                                  "similarTo", "set", "vector", "k_neighbors", "knn_first", "distance", "engine", "metric"])

        self.logger.info(
            f"SPARQL loaded {len(self.connections)} connections and {len(self.properties)} properties")

    def __del__(self):
        from rdflib.plugins.sparql import CUSTOM_EVALS
        if CUSTOM_EVALS.get("aperturedb") == self.eval:
            del CUSTOM_EVALS["aperturedb"]

    def _make_uri(self, prefix, suffix):
        return self.namespaces[prefix] + quote(suffix)

    def _parse_uri(self, uri: "URIRef"):
        from rdflib.term import URIRef
        assert isinstance(uri, URIRef)
        for k, v in self.namespaces.items():
            if uri.startswith(v):
                return k, unquote(uri[len(v):])
        return None, None

    def _parse_uri_with_ns(self, ns: str, uri: "URIRef"):
        from rdflib.term import URIRef
        assert isinstance(uri, URIRef), uri
        assert uri.startswith(self.namespaces[ns]), (ns, uri)
        return unquote(uri[len(self.namespaces[ns]):])

    def _format_node(self, node):
        from rdflib.term import URIRef
        return self.graph.qname(node) if isinstance(node, URIRef) else node.toPython()

    def _format_triple(self, triple):
        return " ".join([self._format_node(node) for node in triple])

    def _format_triples(self, triples):
        return " .\n".join([self._format_triple(triple) for triple in triples])

    def _load_schema(self):
        self.schema = self._utils.get_schema()
        self.connections = {}
        self.connections = {}
        if self.schema is None:
            self.logger.warning("No schema found in ApertureDB")
            return
        if "connections" in self.schema and self.schema["connections"] is not None and "classes" in self.schema["connections"]:
            for c, d in self.schema["connections"]["classes"].items():
                uri = self._make_uri("c", c)
                if uri not in self.connections:
                    self.connections[uri] = (set(), set())
                self.connections[uri][0].add(d["src"])
                self.connections[uri][1].add(d["dst"])
        if not self.connections:
            self.logger.warning("No connections found in schema")

        self.properties = {}
        if "entities" in self.schema and self.schema["entities"] is not None and "classes" in self.schema["entities"]:
            for e, d in self.schema["entities"]["classes"].items():
                for p in d["properties"]:
                    uri = self._make_uri("p", p)
                    if uri not in self.properties:
                        self.properties[uri] = set()
                    self.properties[uri].add(e)
                self.namespaces[f"{e}"] = self._make_uri("o", e + "/")
        if not self.properties:
            self.logger.warning("No properties found in schema")

    def eval(self, ctx: "QueryContext", part: "CompValue"
             ) -> Generator["FrozenBindings", None, None]:
        """
        Evaluate a SPARQL query part

        This method is registered as a custom eval function for rdflib.
        It is given the first opportunity to evaluate a query part.

        Arguments:
            ctx: QueryContext. The query context, supplying existing bindings
            part: CompValue. The query part to evaluate

        Yields:
            FrozenBindings. The results of the query

        Raises:
            NotImplementedError: If the query type is not supported.
                This is the case for all query types except BGP.
        """
        if part.name == 'BGP':
            return self.evalBGP(ctx, part.triples)

        raise NotImplementedError(f"Unsupported query type: {part.name}")

    def evalBGP(self, ctx: "QueryContext",
                triples: List[Tuple["Identifier", "Identifier", "Identifier"]],
                ) -> Generator["FrozenBindings", None, None]:
        """
        Evaluates a Basic Graph Pattern (BGP) query

        Arguments:
            ctx: QueryContext. The query context, supplying existing bindings
            triples: List[Tuple[Identifier, Identifier, Identifier]]. The triples to evaluate

        Yields:
            FrozenBindings. Binding sets that are the results of the query
        """
        def add_find(v, t):
            """Create new Find* command for variable v with type t"""
            from rdflib.term import Variable
            command_name = "FindEntity" if t[0] != "_" else "Find" + t[1:]
            body = {}
            if t[0] != "_":
                body["with_class"] = t
            body["_ref"] = len(query) + 1
            body["uniqueids"] = True
            commands[v] = body
            command = {command_name: body}
            query.append(command)
            if isinstance(v, Variable):
                # print("Adding uniqueid binding for variable", v)
                bindings.append((v, body, "_uniqueid"))
            object_prefixes.append(self._make_uri("o", t + "/"))

        def add_constraint(s, p, o):
            """Add property constraint to existing command"""
            command = commands[s]
            ns, prop = self._parse_uri(p)
            assert ns in ["p", "knn"], (ns, p)
            if "constraints" not in command:
                command["constraints"] = {}
            if prop not in command["constraints"]:
                command["constraints"][prop] = []
            command["constraints"][prop].extend(["==", o.toPython()])

        def add_binding(s, p, o):
            """Associates a return binding variable with a specific property in the ApertureDB response"""
            command = commands[s]
            if "results" not in command:
                command["results"] = {}
            if "list" not in command["results"]:
                command["results"]["list"] = []
            prop = self._parse_uri_with_ns("p", p)
            command["results"]["list"].append(prop)
            bindings.append((o, command, prop))

        def add_is_connected_to(s, p, o):
            """Adds connection constraint between two commands"""
            s_command = commands[s]
            o_command = commands[o]
            if s_command["_ref"] < o_command["_ref"]:
                command = o_command
                ict = dict(
                    ref=s_command["_ref"],
                    direction="out",
                )
                connection_bindings.append((o_command, p, s_command, False))
            else:
                command = s_command
                ict = dict(
                    ref=o_command["_ref"],
                    direction="in",
                )
                connection_bindings.append((s_command, p, o_command, True))

            if p is not None:
                connection_type = self._parse_uri_with_ns("c", p)
                ict["connection_class"] = connection_type
            else:
                del ict["direction"]  # Any connection, either in or out

            if "is_connected_to" not in command:
                command["is_connected_to"] = ict
                command["group_by_source"] = True
            elif list(command["is_connected_to"].keys())[0] == "all":
                list(command["is_connected_to"].values())[0].append(ict)
            else:
                command["is_connected_to"] = {
                    "all": [command["is_connected_to"], ict]}

        def process_bindings(output, ctx: "QueryContext" = ctx, i=0, ids=set()):
            """Processes AperatureDB response and yields bindings

            Operates recursively, processing each command in the query to generate bindings
            for each variable, multiplying the number of bindings at each step.
            Uses `solutions` to avoid yielding duplicate solutions.
            """
            from rdflib.term import Variable, BNode, Literal, URIRef
            if i == len(output):
                solution = ctx.solution()
                # print("Yielding solution", solution)
                if solution in solutions:
                    return
                solutions.add(solution)
                yield solution
                return

            command_body = list(query[i].values())[0]
            result_body = list(output[i].values())[0]
            entities = result_body.get("entities", [])

            bindings2 = [(v, prop) for v, cb, prop in bindings
                         if cb == command_body and not isinstance(v, BNode)]
            assert all(isinstance(v, Variable)
                       for v, _ in bindings2), bindings2
            assert all(ctx[v] is None for v, _ in bindings2), bindings2

            if isinstance(entities, list):
                entities = {None: entities}

            for id_, ee in entities.items():
                if id_ is not None and id_ not in ids:
                    continue
                for e in ee:
                    assert "_uniqueid" in e, e
                    id2 = e["_uniqueid"]
                    ids2 = ids.union({id2})
                    ctx2 = ctx.push()  # not always necessary
                    if bindings2:
                        for v, prop in bindings2:
                            assert prop in e, (prop, e)
                            assert ctx2[v] is None, (v, ctx2[v])
                            if prop == "_uniqueid":
                                uri = URIRef(
                                    object_prefixes[i] + e["_uniqueid"])
                                ctx2[v] = uri
                            else:
                                literal = Literal(e[prop])
                                ctx2[v] = literal
                        yield from process_bindings(output, ctx2, i + 1, ids2)
                    else:
                        yield from process_bindings(output, ctx, i + 1, ids2)

        from rdflib import RDF, Literal
        from rdflib.plugins.sparql.sparql import SPARQLError

        triples = self._apply_context_to_triples(ctx, triples)
        types = self._deduce_types(ctx, triples)
        query = []  # ApertureDB query
        commands = {}  # Mapping from variable to command body
        bindings = []  # List of (variable, command body, property) tuples
        object_prefixes = []  # List of object prefixes
        # List of (source command, connection, destination command, is_forwards) tuples
        connection_bindings = []
        blobs = []  # List of blobs

        # Add Find commands for each variable
        for v, t in types.items():
            add_find(v, t)
        query, object_prefixes = self._optimize_query(query, object_prefixes)

        # Find knn nodes
        knn_nodes = {
            o: s for s, p, o in triples
            if p == self.namespaces["knn"] + "similarTo"}

        # Process triples to generate querty
        # Add constraints, bindings, connections
        for s, p, o in triples:
            if p in self.properties:
                if isinstance(o, Literal):
                    add_constraint(s, p, o)
                else:
                    add_binding(s, p, o)
            elif p in self.connections:
                add_is_connected_to(s, p, o)
            elif p == RDF.type:
                pass
            elif p in self.knn_properties:
                if p == self.namespaces["knn"] + "similarTo":
                    pass
                elif p == self.namespaces["knn"] + "vector":
                    blobs.append(self._decode_descriptor(o))
                elif p == self.namespaces["knn"] + "distance":
                    command["distances"] = True
                    bindings.append((o, command, "_distance"))
                else:
                    s2 = knn_nodes[s]
                    command = commands[s2]
                    prop = self._parse_uri(p)[1]
                    if isinstance(o, Literal):
                        command[prop] = o.toPython()
                    else:
                        add_binding(s2, p, o)
            elif p == self.namespaces["c"] + "ANY":
                add_is_connected_to(s, None, o)
            else:
                raise SPARQLError(
                    f"Unknown property or connection: {p}: valid connections are {self.connections.keys()}, valid properties are {self.properties.keys()}")

        result, output, _ = execute_query(self._client, query, blobs)
        solutions = set()  # process_bindings uses this to avoid yielding duplicate solutions
        yield from process_bindings(output)

        if self.debug:  # Debugging
            self.bindings = bindings
            self.commands = commands
            self.input_query = query
            self.object_prefixes = object_prefixes
            self.output_response = output
            self.solutions = solutions
            self.triples = triples
        self.logger.info(
            f"Evaluated BGP query with {len(triples)} triples, produced {len(commands)} commands, and {len(solutions)} solutions")
        return

    def _decode_descriptor(self, literal: "Literal") -> bytes:
        import numpy as np
        return np.array(json.loads(literal.toPython())).astype(np.float32).tobytes()

    @classmethod
    def encode_descriptor(self, vector: "np.array") -> "Literal":
        from rdflib import Literal
        return Literal(json.dumps(vector.tolist()))

    def _optimize_query(self, query: List[dict], object_prefixes: List["URIRef"]) -> List[dict]:
        """
        Optimize the query by reordering Find commands

        Query should not have connections between Find commands yet
        """
        # Sort by number of constraints, descending, alphabetical by command name
        # TODO: Could use statistics from schema to optimize further
        query, object_prefixes = list(zip(*sorted(zip(query, object_prefixes),
                                                  key=lambda x: (-len(list(x[0].values())[0].get("constraints", [])), list(x[0].keys())[0]))))
        for i, command in enumerate(query, start=1):
            list(command.values())[0]["_ref"] = i
        return query, object_prefixes

    def _apply_context_to_triples(self, ctx: "QueryContext",
                                  triples: List[Tuple["Identifier",
                                                      "Identifier", "Identifier"]]
                                  ) -> List[Tuple["Identifier", "Identifier", "Identifier"]]:
        """
        Apply the query context to the triples
        """
        new_triples = []
        for s, p, o in triples:
            s2 = ctx[s]
            p2 = ctx[p]
            o2 = ctx[o]

            if s2 is not None:
                s = s2
            if p2 is not None:
                p = p2
            if o2 is not None:
                o = o2
            new_triples.append((s, p, o))
        return new_triples

    def _deduce_types(self, cts: "QueryContext",
                      triples: List[Tuple["Identifier",
                                          "Identifier", "Identifier"]]
                      ) -> Dict["Identifier", str]:
        """
        Deduce the type of each subject and connection object in the query
        """
        def add_types(k, tt):
            if k not in types:
                types[k] = tt
            else:
                intersection = types[k].intersection(tt)
                if not intersection:
                    raise SPARQLError(
                        f"Type error: {k} does not match a type {types[k]} vs {tt}")
                types[k] = intersection

        from rdflib import RDF
        from rdflib.plugins.sparql.sparql import QueryContext, SPARQLError
        types = {}
        for s, p, o in triples:
            s_type = self._deduce_type(s)
            if s_type:
                add_types(s, {s_type})

            if p in self.properties:
                add_types(s, self.properties[p])
            elif p in self.connections:
                src, dst = self.connections[p]
                o_type = self._deduce_type(o)
                if o_type:
                    add_types(o, [o_type])
                add_types(s, src)
                add_types(o, dst)
            elif p == RDF.type:
                add_types(s, [self._parse_uri_with_ns('t', o)])
            elif p in self.knn_properties:
                if p == self.namespaces["knn"] + "similarTo":
                    add_types(s, {"_Descriptor"})
            elif p == self.namespaces["c"] + "ANY":
                pass
            else:
                raise SPARQLError(
                    f"Unknown property or connection: {p}: valid connections are {self.connections.keys()}, valid properties are {self.properties.keys()}")

        for v, t in types.items():
            if len(t) > 1:
                raise SPARQLError(f"Ambiguous type for {v}: {t}")

        return {v: list(t)[0] for v, t in types.items()}

    def _deduce_type(self, v: "Identifier") -> str:
        """
        Deduce the type of an identifier based on its URI
        """
        from rdflib.term import URIRef
        if isinstance(v, URIRef) and v.startswith(self.namespaces["o"]):
            local_name = self._parse_uri_with_ns("o", v)
            return local_name.split("/")[0]
        return None

    def _deduce_uniqueid(self, v: "Identifier") -> str:
        """
        Deduce the uniqueid of an identifier based on its URI
        """
        from rdflib.term import URIRef
        if isinstance(v, URIRef) and v.startswith(self.namespaces["o"]):
            local_name = self._parse_uri_with_ns("o", v)
            return local_name.split("/")[1]
        return None

    def query(self, query: str):
        """
        Execute a SPARQL query
        """
        return self.graph.query(query)

    def to_dataframe(self, result) -> "pd.DataFrame":
        """
        Convert the SPARQL result to a pandas DataFrame
        """
        import pandas as pd

        return pd.DataFrame(
            data=([None if x is None else self._format_node(x)
                   for x in row] for row in result),
            columns=[str(x) for x in result.vars],
        )

    def get_blob(self, uri: Union[str, "URIRef"], type: Optional[str] = None) -> bytes:
        """
        Get the blob associated with a URI or QName.
        """
        if isinstance(uri, str):
            uri = self.graph.namespace_manager.expand_curie(uri)

        if type is None:
            type = self._deduce_type(uri)
        else:
            assert type == self._deduce_type(
                uri), f"Type {type} does not match deduced type {self._deduce_type(uri)}"
        assert type is not None, f"Cannot get blob for entity URI: {uri}"
        assert type[0] == "_", f"Cannot get blob for entity URI: {uri} with type {type}"
        uniqueid = self._deduce_uniqueid(uri)
        command_name = "Find" + type[1:]
        query = [
            {command_name: {
                "constraints": {
                    "_uniqueid": ["==", uniqueid],
                },
                "blobs": True,
            }},
        ]
        _, blobs = self._client.query(query)
        return blobs[0]

    def get_blobs(self, uris: List[Union[str, "URIRef"]], type: Optional[str] = None) -> List[bytes]:
        """
        Get the blobs associated with a list of URI or QName.
        """
        uris = [self.graph.namespace_manager.expand_curie(uri)
                if isinstance(uri, str) else uri for uri in uris]
        types = [self._deduce_type(uri) for uri in uris]
        if type is not None:
            assert all(
                t == type for t in types), f"Type {type} does not match deduced types {types}"
        else:
            type = types[0]
            assert all(
                t == type for t in types), f"Types do not match: {types}"

        assert type is not None, f"Cannot get blob for entity URI: {uri}"
        assert type[0] == "_", f"Cannot get blob for entity URI: {uri} with type {type}"
        uniqueids = [self._deduce_uniqueid(uri) for uri in uris]
        command_name = "Find" + type[1:]
        query = [
            {command_name: {
                "results": {"list": ["_uniqueid"]},
                "constraints": {
                    "_uniqueid": ["in", uniqueids],
                },
                "blobs": True,
            }},
        ]
        res, blobs = self._client.query(query)
        # Put blobs in a dictionary for faster lookup
        entities = res[0][command_name].get("entities", [])
        id_blobs = {e["_uniqueid"]: blobs[e["_blob_index"]] for e in entities}
        # Put the results in the same order as the inputs
        results = [id_blobs.get(uniqueid) for uniqueid in uniqueids]
        if any(blob is None for blob in results):
            self.logger.warning("Some blobs were not found")
        return results

    def get_image(self, uri: Union[str, "URIRef"]) -> "np.ndarray":
        """
        Get the image associated with a URI or QName
        """
        import numpy as np
        import cv2

        blob = self.get_blob(uri, type="_Image")
        nparr = np.fromstring(blob, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return image

    def get_images(self, uris: List[Union[str, "URIRef"]]) -> List["np.ndarray"]:
        """
        Get the images associated with a list of URI or QName
        """
        import numpy as np
        import cv2

        blobs = self.get_blobs(uris, type="_Image")
        images = []
        for blob in blobs:
            if blob is not None:
                nparr = np.fromstring(blob, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                images.append(image)
            else:
                images.append(None)
        return images

    def show_images(self, uris: List[Union[str, "URIRef"]]):
        """
        Show the images associated with a list of URI or QName

        This is best used in a Jupyter notebook.
        """
        import matplotlib.pyplot as plt

        images = self.get_images(uris)
        columns = min(5, len(images))
        rows = math.ceil(len(images) / columns)
        for i, image in enumerate(images):
            plt.subplot(rows, columns, i + 1)
            if image is not None:
                plt.imshow(image)
            plt.axis("off")
        plt.show()

    def get_descriptor(self, uri: Union[str, "URIRef"]) -> "np.ndarray":
        """
        Get the descriptor associated with a URI or QName
        """
        import numpy as np
        blob = self.get_blob(uri, type="_Descriptor")
        return np.frombuffer(blob, dtype=np.float32)

    def get_descriptors(self, uris: List[Union[str, "URIRef"]]) -> List["np.ndarray"]:
        """
        Get the descriptors associated with a list of URI or QName
        """
        import numpy as np
        blobs = self.get_blobs(uris, type="_Descriptor")
        return [np.frombuffer(blob, dtype=np.float32) for blob in blobs]
