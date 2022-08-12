from itertools import combinations
import logging
import pytest
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Subscriptable import Subscriptable
import math

logger = logging.getLogger(__name__)


class EntityWithResponseDataCSV(EntityDataCSV):
    def __init__(self, filename, requests, responses):
        super().__init__(filename)
        self.requests = requests
        self.responses = responses

    def response_handler(self, request, input_blob, response, output_blob):
        self.requests.append(request)
        self.responses.append(response)


class GeneratorWithNCommands(Subscriptable):
    def __init__(self, commands_per_query=1, blobs_per_query=0, elements=100) -> None:
        super().__init__()
        self.commands_per_query = commands_per_query
        self.blobs_per_query = blobs_per_query
        self.elements = elements

    def __len__(self):
        return self.elements

    def getitem(self, subscript):
        query = []
        blobs = []
        for i in range(self.commands_per_query):
            query.append({
                "FindEntity": {

                }
            })
        buff = None
        with open(__file__, "rb") as ins:
            buff = ins.read()

        for i in range(self.blobs_per_query):
            blobs.append(buff)

        return query, blobs


class QGPersons(Subscriptable):
    def __init__(self, requests, responses, cpq) -> None:
        super().__init__()
        self.requests = requests
        self.responses = responses
        self.cpq = cpq

    def __len__(self):
        return math.ceil(10 / self.cpq)

    def getitem(self, subscript):
        query = []
        for i in range(self.cpq):
            query.append(
                {
                    "FindEntity": {
                        "with_class": "Person",
                        "results": {
                            "all_properties": True
                        },
                        "constraints": {
                            "age": [">=", (subscript * self.cpq + i) * 10, "<", (subscript * self.cpq + 1 + i) * 10]
                        }
                    }
                }
            )
            if subscript * self.cpq + 1 + i == 10:
                break
        return query, []

    def response_handler(self, request, input_blob, response, output_blob):
        self.requests.append(request)
        self.responses.append(response)


class TestResponseHandler():
    def cleanDB(self, utils):
        logger.debug(f"Cleaning existing data")
        assert utils.remove_entities("_BoundingBox") == True
        assert utils.remove_entities("Person") == True
        self.requests = []
        self.responses = []

    @pytest.mark.parametrize("records_count", [(1), (99), (100)])
    def test_response_handler(self, records_count, db, utils):
        self.cleanDB(utils=utils)
        logger.warning(f"{records_count}")
        persons = EntityWithResponseDataCSV(
            "./input/persons.adb.csv", self.requests, self.responses)
        persons = persons[:records_count]
        loader = ParallelLoader(db)
        loader.ingest(persons, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert loader.error_counter == 0

        assert len(persons) == records_count
        logger.info(self.responses)
        assert len(self.responses) == records_count
        logger.info(self.requests)
        assert len(self.requests) == records_count

    @pytest.mark.parametrize("commands_blobs", combinations(range(1, 4), 2))
    def test_varying_commands_blobs(self, db, commands_blobs):
        i, j  = commands_blobs
        generator = GeneratorWithNCommands(i, j)
        querier = ParallelQuery(db, dry_run=True)
        querier.query(generator, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert querier.commands_per_query == i
        assert querier.blobs_per_query == j

    @pytest.mark.parametrize("cpq", range(1, 11))
    def test_varying_response(self, db, utils, cpq):
        self.cleanDB(utils=utils)
        persons = EntityWithResponseDataCSV(
            "./input/persons.adb.csv", self.requests, self.responses)
        loader = ParallelLoader(db)
        loader.ingest(persons, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert loader.error_counter == 0

        dist = persons.df.groupby(persons.df.age // 10 * 10).count()
        logger.debug(dist["age"])

        self.requests = []
        self.responses = []
        generator = QGPersons(self.requests, self.responses, cpq)
        querier = ParallelQuery(db)
        querier.query(generator, batchsize=100,
                      numthreads=1,
                      stats=True)
        dist_by_ages = {}
        for req, resp in zip(self.requests, self.responses):
            logger.debug(req)
            assert len(req) in [cpq, 10 % cpq]
            for rq, rr in zip(req, resp):
                for key in rq:
                    age_group = rq[key]["constraints"]["age"][1]
                    count = rr[key]["returned"]
                    dist_by_ages[age_group] = count
        logger.debug(dist_by_ages)
        for k in dist_by_ages:
            assert dist_by_ages[k] == dist["age"][k]
