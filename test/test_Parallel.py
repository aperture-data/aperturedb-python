import logging
import random

from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Subscriptable import Subscriptable

logger = logging.getLogger(__name__)

# Tests for parallel which don't involve data.


class GeneratorWithErrors(Subscriptable):
    def __init__(self, commands_per_query=1, elements=100, error_pct=.5) -> None:
        super().__init__()
        self.commands_per_query = commands_per_query
        self.elements = elements
        self.error_pct = error_pct

    def __len__(self):
        return self.elements

    def getitem(self, subscript):
        query = []
        blobs = []
        for i in range(self.commands_per_query):
            if random.randint(0, 100) <= (self.error_pct * 100):
                query.append({
                    "BadCommand": {
                    }
                })
            else:
                query.append({
                    "FindEntity": {
                        "results": {
                            "count": True
                        }
                    }
                })

        return query, blobs


class TestParallel():
    """
    These check operation of ParallelQuery
    """

    def test_someBadQueries(self, db: Connector):
        """
        Verifies that it handles some queries returning errors
        """
        try:
            elements = 100
            generator = GeneratorWithErrors(elements=elements)
            querier = ParallelQuery(db, dry_run=False)
            querier.query(generator, batchsize=2,
                          numthreads=8,
                          stats=True)
            logger.info(querier.get_succeeded_commands())
            assert querier.get_succeeded_commands() < elements
        except Exception as e:
            print(e)
            print("Failed to renew Session")
            assert False

    def test_allBadQueries(self, db: Connector):
        """
        Verifies that it handles all queries returning errors
        """
        try:
            elements = 100
            generator = GeneratorWithErrors(elements=elements, error_pct=1)
            querier = ParallelQuery(db, dry_run=False)
            querier.query(generator, batchsize=2,
                          numthreads=8,
                          stats=True)
            logger.info(querier.get_succeeded_commands())
            assert querier.get_succeeded_commands() == 0
        except Exception as e:
            print(e)
            print("Failed to renew Session")
            assert False

    def test_dictResponseHandling(self):
        """
        Verifies that it handles a dict response from a failing server properly.
        Guards against regression to "unhashable type: 'slice'" when r is a dict.
        """
        from unittest.mock import patch, MagicMock
        try:
            elements = 10
            generator = GeneratorWithErrors(elements=elements)
            db = MagicMock(spec=Connector)
            db.clone.return_value = db
            db.config = "mock_config"
            db.query.return_value = ([{"GetSchema": {"status": 0}}], [])
            querier = ParallelQuery(db, dry_run=False)
            
            # Now set the mock for the actual queries
            db.query.return_value = ({"status": 3, "error_msg": "mock error"}, [])
            db.last_query_ok.return_value = True
            db.get_last_query_time.return_value = 1.0
            
            querier.query(generator, batchsize=2, numthreads=1, stats=True)
            
            # Since all queries got status 3, no commands or queries succeeded.
            assert querier.get_succeeded_commands() == 0
            assert querier.get_succeeded_queries() == 0
            # Ensure stats were recorded properly
            assert len(querier.actual_stats) > 0
        except Exception as e:
            print(e)
            assert False
