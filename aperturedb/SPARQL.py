# SPARQL wrapper for ApertureDB
#
# This script allows ApertureDB to be queried using SPARQL queries.
# It is based on the rdflib library.
# Currently, it supports a subset of BGP queries, but it can be extended to support more complex queries.

from typing import Dict, Generator, List, Tuple, Union
from urllib.parse import quote, unquote
import json

import rdflib
from rdflib.term import BNode, Identifier, Literal, URIRef, Variable
from rdflib.plugins.sparql.sparql import (
    AlreadyBound,
    FrozenBindings,
    FrozenDict,
    Query,
    QueryContext,
    SPARQLError,
)
from rdflib.plugins.sparql.parserutils import CompValue
import pandas as pd

from aperturedb.CommonLibrary import create_connector
from aperturedb.Utils import Utils


class SPARQL:
    def __init__(self, client=None):
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
        }
        self._load_schema()
        # TODO: Only one instance of SPARQL can be used at a time
        rdflib.plugins.sparql.CUSTOM_EVALS["aperturedb"] = self.eval
        self.graph = rdflib.Graph()
        for k, v in self.namespaces.items():
            self.graph.bind(k, v)

    def _make_uri(self, prefix, suffix):
        return self.namespaces[prefix] + quote(suffix)

    def _parse_uri(self, uri: URIRef):
        assert isinstance(uri, URIRef)
        for k, v in self.namespaces.items():
            if uri.startswith(v):
                return k, unquote(uri[len(v):])
        return None, None

    def _parse_uri_with_ns(self, ns: str, uri: URIRef):
        assert isinstance(uri, URIRef)
        assert uri.startswith(self.namespaces[ns])
        return unquote(uri[len(self.namespaces[ns]):])

    def _format_node(self, node):
        return self.graph.qname(node) if isinstance(node, URIRef) else node.toPython()

    def _format_triple(self, triple):
        return " ".join([self._format_node(node) for node in triple])

    def _format_triples(self, triples):
        return " .\n".join([self._format_triple(triple) for triple in triples])

    def _load_schema(self):
        self.schema = self._utils.get_schema()
        self.connections = {}
        self.connections = {}
        for c, d in self.schema["connections"]["classes"].items():
            uri = self._make_uri("c", c)
            if uri not in self.connections:
                self.connections[uri] = (set(), set())
            self.connections[uri][0].add(d["src"])
            self.connections[uri][1].add(d["dst"])

        self.properties = {}
        for e, d in self.schema["entities"]["classes"].items():
            for p in d["properties"]:
                uri = self._make_uri("p", p)
                if uri not in self.properties:
                    self.properties[uri] = set()
                self.properties[uri].add(e)
            self.namespaces[f"{e}"] = self._make_uri("o", e + "/")

    def eval(self, ctx: QueryContext, part: CompValue
             ) -> Generator[FrozenBindings, None, None]:
        """
        Execute a SPARQL query on the graph
        """
        if part.name == 'BGP':
            return self.evalBGP(ctx, part.triples)

        raise NotImplementedError(f"Unsupported query type: {part.name}")

    def evalBGP(self, ctx: QueryContext,
                triples: List[Tuple[Identifier, Identifier, Identifier]],
                ) -> Generator[FrozenBindings, None, None]:
        """
        Execute a SPARQL query on the graph
        """
        def add_find(v, t):
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
            command = commands[s]
            prop = self._parse_uri_with_ns("p", p)
            if "constraints" not in command:
                command["constraints"] = {}
            if prop not in command["constraints"]:
                command["constraints"][prop] = []
            command["constraints"][prop].extend(["==", o.toPython()])

        def add_binding(s, p, o):
            command = commands[s]
            if "results" not in command:
                command["results"] = {}
            if "list" not in command["results"]:
                command["results"]["list"] = []
            prop = self._parse_uri_with_ns("p", p)
            command["results"]["list"].append(prop)
            bindings.append((o, command, prop))

        def add_is_connected_to(s, p, o):
            s_command = commands[s]
            o_command = commands[o]
            connection_type = self._parse_uri_with_ns("c", p)
            if s_command["_ref"] < o_command["_ref"]:
                command = o_command
                ict = dict(
                    ref=s_command["_ref"],
                    connection_class=connection_type,
                    direction="out",
                )
                connection_bindings.append((o_command, p, s_command, False))
            else:
                command = s_command
                ict = dict(
                    ref=o_command["_ref"],
                    connection_class=connection_type,
                    direction="in",
                )
                connection_bindings.append((s_command, p, o_command, True))

            if "is_connected_to" not in command:
                command["is_connected_to"] = ict
                command["group_by_source"] = True
            elif list(command["is_connected_to"].keys())[0] == "all":
                list(command["is_connected_to"].values())[0].append(ict)
            else:
                command["is_connected_to"] = {
                    "all": [command["is_connected_to"], ict]}

        def process_bindings(output, ctx: QueryContext = ctx, i=0, ids=set()):
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
            # print(
            #     f"Processing bindings {i=} {command_body=} {result_body=} {len(entities)=} {bindings2=} {ids=}")

            if isinstance(entities, list):
                entities = {None: entities}

            # print(f"Processing entities {entities=}")
            for id_, ee in entities.items():
                # print(f"Processing entities {id_=} {ee=}")
                if id_ is not None and id_ not in ids:
                    # print("Skipping", id_, ids)
                    continue
                # print(f"Iterating over entities {ee=}")
                for e in ee:
                    # print(f"Processing entity {e=} {bindings2=}")
                    assert "_uniqueid" in e, e
                    id2 = e["_uniqueid"]
                    # print(f"{id2=}")
                    ids2 = ids.union({id2})
                    # print(f"{ids=} {ids2=}")
                    ctx2 = ctx.push()  # not always necessary
                    # print(f"{ctx=} {ctx2=}")
                    if bindings2:
                        # print("Processing bindings", bindings2)
                        for v, prop in bindings2:
                            # print(f"Processing binding {v=} {prop=}")
                            assert prop in e, (prop, e)
                            assert ctx2[v] is None, (v, ctx2[v])
                            if prop == "_uniqueid":
                                uri = URIRef(
                                    object_prefixes[i] + e["_uniqueid"])
                                # print(f"{v} <- {uri}")
                                ctx2[v] = uri
                            else:
                                literal = Literal(e[prop])
                                # print(f"{v} <- {literal}")
                                ctx2[v] = literal
                        yield from process_bindings(output, ctx2, i+1, ids2)
                    else:
                        # print("No bindings")
                        yield from process_bindings(output, ctx, i+1, ids2)

        self.triples = triples  # Save triples for debugging
        triples = self._apply_context_to_triples(ctx, triples)
        types = self._deduce_types(ctx, triples)
        # print(f"Types: {types}")

        query = []  # ApertureDB query
        commands = {}  # Mapping from variable to command body
        bindings = []  # List of (variable, command body, property) tuples
        object_prefixes = []  # List of object prefixes
        # List of (source command, connection, destination command, is_forwards) tuples
        connection_bindings = []
        for v, t in types.items():
            add_find(v, t)

        # print("Query before properties", query)

        property_triples = [(s, p, o)
                            for s, p, o in triples if p in self.properties]
        connection_triples = [(s, p, o)
                              for s, p, o in triples if p in self.connections]
        type_triples = [(s, p, o)
                        for s, p, o in triples if p == rdflib.RDF.type]

        assert len(property_triples) + len(connection_triples) \
            + len(type_triples) == len(triples), "Found unknown triples"

        for s, p, o in property_triples:
            if isinstance(o, Literal):
                add_constraint(s, p, o)
            else:
                add_binding(s, p, o)

        query = self._optimize_query(query)

        for s, p, o in connection_triples:
            add_is_connected_to(s, p, o)

        # print("Query after connections", query)

        self.input_query = query  # Save query for debugging
        output, _ = self._client.query(query)
        self.output_response = output  # Save response for debugging
        # print("Output", json.dumps(output, indent=2))

        # print("Processing bindings")
        solutions = set()
        yield from process_bindings(output)
        self.solutions = solutions  # Save solutions for debugging

        # raise NotImplementedError("Query execution not implemented")
        return

    def _optimize_query(self, query: List[dict]) -> List[dict]:
        """
        Optimize the query by reordering Find commands

        Query should not have connections between Find commands yet
        """
        # Sort by number of constraints, descending
        # TODO: Could use statistics from schema to optimize further
        query = sorted(query,
                       key=lambda x: -len(list(x.values())[0].get("constraints", [])))
        for i, command in enumerate(query, start=1):
            list(command.values())[0]["_ref"] = i
        return query

    def _apply_context_to_triples(self, ctx: QueryContext, triples: List[Tuple[Identifier, Identifier, Identifier]]
                                  ) -> List[Tuple[Identifier, Identifier, Identifier]]:
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

    def _deduce_types(self, cts: QueryContext,
                      triples: List[Tuple[Identifier, Identifier, Identifier]]
                      ) -> Dict[Identifier, str]:
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

        types = {}
        for s, p, o in triples:
            s_type = self._deduce_type(s)
            if s_type:
                add_types(s, [s_type])

            if p in self.properties:
                add_types(s, self.properties[p])
            elif p in self.connections:
                src, dst = self.connections[p]
                o_type = self._deduce_type(o)
                if o_type:
                    add_types(o, [o_type])
                add_types(s, src)
                add_types(o, dst)
            elif p == rdflib.RDF.type:
                add_types(s, [self._parse_uri_with_ns('t', o)])
            else:
                raise SPARQLError(
                    f"Unknown property or connection: {p}: valid connections are {self.connections.keys()}, valid properties are {self.properties.keys()}")

        for v, t in types.items():
            if len(t) > 1:
                raise SPARQLError(f"Ambiguous type for {v}: {t}")

        return {v: list(t)[0] for v, t in types.items()}

    def _deduce_type(self, v: Identifier) -> str:
        """
        Deduce the type of an identifier based on its URI
        """
        if isinstance(v, URIRef) and v.startswith(self.namespaces["o"]):
            local_name = self._parse_uri_with_ns("o", v)
            return local_name.split("/")[0]
        return None

    def _deduce_uniqueid(self, v: Identifier) -> str:
        """
        Deduce the uniqueid of an identifier based on its URI
        """
        if isinstance(v, URIRef) and v.startswith(self.namespaces["o"]):
            local_name = self._parse_uri_with_ns("o", v)
            return local_name.split("/")[1]
        return None

    def query(self, query: str):
        """
        Execute a SPARQL query
        """
        return self.graph.query(query)

    def to_dataframe(self, result):
        """
        Convert the SPARQL result to a pandas DataFrame
        """
        return pd.DataFrame(
            data=([None if x is None else self._format_node(x)
                   for x in row] for row in result),
            columns=[str(x) for x in result.vars],
        )

    def get_blob(self, uri: Union[str, URIRef]) -> bytes:
        """
        Get the blob associated with a URI or QName.
        """
        if isinstance(uri, str):
            uri = self.graph.namespace_manager.expand_curie(uri)

        type = self._deduce_type(uri)
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

    def get_image(self, uri: Union[str, URIRef]) -> "np.ndarray":
        """
        Get the image associated with a URI or QName
        """
        import numpy as np
        import cv2

        blob = self.get_blob(uri)
        nparr = np.fromstring(blob, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        return image

    def show_image(self, uri: Union[str, URIRef]):
        """
        Show the image associated with a URI or QName

        This is best used in a Jupyter notebook.
        """
        import matplotlib.pyplot as plt

        image = self.get_image(uri)
        plt.imshow(image)
        plt.axis("off")
        plt.show()
