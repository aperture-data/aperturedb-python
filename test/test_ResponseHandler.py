from itertools import combinations
import logging
import pytest
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ParallelLoader import ParallelLoader
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.ParallelQuerySet import ParallelQuerySet
from aperturedb.Subscriptable import Subscriptable
from aperturedb.Connector import Connector
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
    def __init__(self, requests, responses, cpq, response_blobs) -> None:
        super().__init__()
        self.requests = requests
        self.responses = responses
        self.response_blobs = response_blobs
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
        if output_blob is not None and len(output_blob) > 0:
            self.response_blobs.append(output_blob)


class QGPersonsBadHandler(QGPersons):
    def response_handler(self, request, input_blob):
        self.requests.append(request)
        self.responses.append(response)
        if output_blob is not None and len(output_blob) > 0:
            self.response_blobs.append(output_blob)


class QGPersonsIndex(QGPersons):
    def response_handler(self, request, input_blob, response, output_blob, index):
        self.requests[index] = request
        self.responses[index] = response
        if output_blob is not None and len(output_blob) > 0:
            self.response_blobs[index] = output_blob


class QGImages(QGPersons):
    def getitem(self, subscript):
        query = []
        for i in range(self.cpq):
            q = {
                "FindImage": {
                    "blobs": True
                }
            }
            query.append(q)
        return query, []

# mimics a Entity connected to an Image


class QGSetPersonAndImages(Subscriptable):
    def __init__(self, requests, responses, max_commands, request_blobs) -> None:
        super().__init__()
        self.requests = requests
        self.responses = responses
        self.request_blobs = request_blobs
        self.max_commands = max_commands
        self.commands_per_query = [1, 1]
        self.blobs_per_query = [0, 1]

    def __len__(self):
        return self.max_commands

    def getitem(self, subscript):
        query_set = []
        entityquery = {
            "AddEntity": {
                "with_class": "Person",
                "properties": {
                    "age": (subscript + 20)
                },
                "constraint": {
                    "age": ["==", (subscript + 20)]
                }
            }
        }
        # add image if entity doesn't exist.
        image_constraint = {"results": {0: {"status": ["!=", 2]}}}

        imagequery = {
            "AddImage": {
                "properties": {
                    "type": "portrait",
                    "age": (subscript + 20)
                }
            }
        }
        query_set.append(entityquery)
        query_set.append([image_constraint, imagequery])

        entity_blobs = []
        image_blobs = [subscript]
        set_blobs = [entity_blobs, image_blobs]
        return [query_set], [set_blobs]

    def response_handler(self, set_id, request, input_blob, response, output_blob):
        if not set_id in self.requests:
            self.requests[set_id] = []
            self.responses[set_id] = []
            self.request_blobs[set_id] = []
        self.requests[set_id].append(request)
        self.responses[set_id].append(response)
        # we attach 1 input "blob",so take it out of the list.
        if input_blob is not None and len(input_blob) > 0:
            self.request_blobs[set_id].append(input_blob[0])


# Fake DB query.
def mock_query(self, request, blobs):
    self.response = [{k: {"status": 0} for c in request for k, v in c.items()}]
    return self.response, []

# Fake DB query. Depending in the transaction, return a response increasing number of images.
# Also add a item (int) to the blobs list for each image returned (to simulate the image data)
# The number of blobs returned is validated at the end.


def query_mocker_factory(response_blobs_count):
    def mock_query(self, request, blobs):
        self.response = 0
        mock_responses = [{
            "FindImage": {
                "returned": (i + 1),
                "status": 0
            }
        } for i, c in enumerate(request)]
        mock_blobs = [i for i in range(
            int(response_blobs_count * len(request)))]
        return mock_responses, mock_blobs
    return mock_query


def set_query_mocker_factory(response_blobs_count):
    # we return that entity "exists" for odd numbered
    # requests, this allows us to verify the results
    #  properly filter passed in blobs.
    # also return 0 for addimage.
    def mock_query(self, request, blobs):
        cmd_is_image = "AddImage" in request[0].keys()
        cmd = "AddImage" if cmd_is_image else "AddEntity"

        mock_responses = [{
            cmd: {
                "returned": (i + 1),
                "status": 0 if i % 2 == 0 or cmd_is_image else 2
            }
        } for i, c in enumerate(request)]
        mock_blobs = [i for i in range(
            int(response_blobs_count * len(request)))]
        self.response = mock_responses
        return mock_responses, mock_blobs if cmd_is_image else []
    return mock_query


class TestResponseHandler():
    def cleanDB(self):
        # we no longer clean db since we insert no data, using mock instead.
        logger.debug(f"Cleaning existing data")
        self.requests = []
        self.responses = []

    @pytest.mark.parametrize("records_count", [(1), (99), (100)])
    def test_response_handler(self, records_count, db, utils, monkeypatch):
        monkeypatch.setattr(Connector, "query", lambda s,
                            r, b: mock_query(s, r, b))
        self.cleanDB()
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
    def test_varying_commands_blobs(self, db, commands_blobs, monkeypatch):
        monkeypatch.setattr(Connector, "query", lambda s,
                            r, b: mock_query(s, r, b))
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
        self.cleanDB()
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
        self.response_blobs = []
        generator = QGPersons(self.requests, self.responses,
                              cpq, self.response_blobs)
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

    def test_response_blobs(self, db, utils, monkeypatch):
        # 5.5 makes blob result uneven number compared to query count
        monkeypatch.setattr(Connector, "query", lambda s, r,
                            b: query_mocker_factory(5.5)(s, r, b))
        self.cleanDB()
        self.response_blobs = []
        generator = QGImages(self.requests, self.responses,
                             1, self.response_blobs)
        querier = ParallelQuery(db)
        querier.query(generator, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert len(self.response_blobs) == len(self.responses)
        for i, resp in enumerate(self.responses):
            for key in resp[0]:
                assert resp[0][key]["returned"] == i + 1
                assert resp[0][key]["status"] == 0
                assert len(self.response_blobs[i]) == resp[0][key]["returned"]

    @pytest.mark.parametrize("max_commands", range(1, 7, 3))
    def test_set_response(self, db, utils, max_commands, monkeypatch):
        monkeypatch.setattr(Connector, "query", lambda s, r,
                            b: set_query_mocker_factory(5.5)(s, r, b))
        self.cleanDB()
        self.requests = {}
        self.responses = {}
        self.response_blobs = {}
        generator = QGSetPersonAndImages(self.requests, self.responses,
                                         max_commands, self.response_blobs)
        querier = ParallelQuerySet(db)
        querier.query(generator, batchsize=99,
                      numthreads=31,
                      stats=True)

        assert querier.error_counter == 0
        for i, set_key in enumerate(self.responses):
            if set_key in self.response_blobs:
                expected_blobs = 0
                # each resp is the query response back from that query
                for j, resp in enumerate(self.responses[set_key]):
                    for part in resp:
                        key = list(part.keys())[0]
                        expected_status = 0
                        if key == "AddEntity" and j % 2 == 1:
                            expected_status = 2
                        assert part[key]["status"] == expected_status
                        if key == "AddImage":
                            assert (self.response_blobs[set_key][j]
                                    == j * 2)
                            expected_blobs = expected_blobs + 1
                        else:
                            assert part[key]["returned"] == j + 1
                assert len(self.response_blobs[set_key]) == expected_blobs

    @pytest.mark.parametrize("cpq", range(1, 3))
    def test_varying_response_w_index(self, db, utils, cpq):
        self.cleanDB()
        persons = EntityWithResponseDataCSV(
            "./input/persons.adb.csv", self.requests, self.responses)
        loader = ParallelLoader(db)
        loader.ingest(persons, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert loader.error_counter == 0

        self.requests = {}
        self.responses = {}
        self.response_blobs = {}
        generator = QGPersonsIndex(self.requests, self.responses,
                                   cpq, self.response_blobs)
        querier = ParallelQuery(db)
        querier.query(generator, batchsize=100,
                      numthreads=1,
                      stats=True)
        # req and resp are dict mapping index number to query.
        for reqk, respk in zip(self.requests.keys(), self.responses.keys()):
            req = self.requests[reqk]
            age_group = req[0]["FindEntity"]["constraints"]["age"][1]
            # in QGPersonIndex, age_group[1] is created by taking index
            # and multiplying it by cqp and 10.
            assert reqk == (age_group // cpq // 10)

    def test_bad_responsehandler(self, db, utils, monkeypatch):
        monkeypatch.setattr(Connector, "query", lambda s,
                            r, b: mock_query(s, r, b))
        self.requests = []
        self.responses = []
        self.response_blobs = []
        generator = QGPersonsBadHandler(self.requests, self.responses,
                                        1, self.response_blobs)
        querier = ParallelQuery(db)
        querier.query(generator, batchsize=99,
                      numthreads=31,
                      stats=True)
        assert querier.error_counter != 0
