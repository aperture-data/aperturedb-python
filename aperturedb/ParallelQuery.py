from __future__ import annotations
from typing import Callable
from aperturedb import Parallelizer
import numpy as np
import json
import logging
import math


from aperturedb.DaskManager import DaskManager


logger = logging.getLogger(__name__)


def execute_batch(q, blobs, db, success_statuses: list[int] = [0],
                  response_handler: Callable = None, commands_per_query: int = 1, blobs_per_query: int = 0,
                  strict_response_validation: bool = False):
    """
    Execute a batch of queries, doing useful logging around it.
    Calls the response handler if provided.
    This should be used (without the parallel machinery) istead of db.query to keep the
    response handling consistent, better logging, etc.

    Returns:
        - 0 : if all commands succeeded
        - 1 : if there was -1 in the response
        - 2 : For any other code.
    """
    result = 0
    logger.debug(f"Query={q}")
    r, b = db.query(q, blobs)
    logger.debug(f"Response={r}")

    if db.last_query_ok():
        if response_handler is not None:
            # We could potentially always call this handler function
            # and let the user deal with the error cases.
            blobs_returned = 0
            for i in range(math.ceil(len(q) / commands_per_query)):
                start = i * commands_per_query
                end = start + commands_per_query
                blobs_start = i * blobs_per_query
                blobs_end = blobs_start + blobs_per_query

                b_count = 0
                for req, resp in zip(q[start:end], r[start:end]):
                    for k in req:
                        # Ref to https://docs.aperturedata.io/query_language/Reference/shared_command_parameters/blobs
                        blobs_where_default_true = \
                            k in ["FindImage", "FindBlob"] and (
                                "blobs" not in req[k] or req[k]["blobs"])
                        blobs_where_default_false = \
                            k in [
                                "FindDescriptor", "FindBoundingBox"] and "blobs" in req[k] and req[k]["blobs"]
                        if blobs_where_default_true or blobs_where_default_false:
                            count = resp[k]["returned"]
                            b_count += count

                try:
                    response_handler(
                        q[start:end],
                        blobs[blobs_start:blobs_end],
                        r[start:end],
                        b[blobs_returned:blobs_returned + b_count] if len(b) < blobs_returned + b_count else None)
                except BaseException as e:
                    logger.exception(e)
                    if strict_response_validation:
                        raise e
                blobs_returned += b_count
    else:
        # Transaction failed entirely.
        logger.error(f"Failed query = {q} with response = {r}")
        result = 1

    statuses = {}
    if isinstance(r, dict):
        statuses[r['status']] = [r]
    elif isinstance(r, list):
        # add each result to a list of the responses, keyed by the response
        # code.
        [statuses.setdefault(result[cmd]['status'], []).append(result)
         for result in r for cmd in result]
    else:
        logger.error("Response in unexpected format")
        result = 1

    # last_query_ok means result status >= 0
    if result != 1:
        warn_list = []
        for status, results in statuses.items():
            if status not in success_statuses:
                for wr in results:
                    warn_list.append(wr)
        if len(warn_list) != 0:
            logger.warning(
                f"Partial errors:\r\n{json.dumps(q)}\r\n{json.dumps(warn_list)}")
            result = 2

    return result, r, b


class ParallelQuery(Parallelizer.Parallelizer):
    """
    **Parallel and Batch Querier for ApertureDB**

    This class provides the abstraction for partitioning data into batches,
    so that they may be processed using different threads.
    """

    # 0 is success, 2 is object exists
    success_statuses = [0, 2]

    @classmethod
    def setSuccessStatus(cls, statuses: list[int]):
        cls.success_statuses = statuses

    @classmethod
    def getSuccessStatus(cls):
        return cls.success_statuses

    def __init__(self, db, dry_run=False):

        super().__init__()

        self.db = db.create_new_connection()

        self.dry_run = dry_run

        self.type = "query"

        self.responses = []

        self.commands_per_query = 1
        self.blobs_per_query = 0
        self.daskManager = None
        self.batch_command = execute_batch

    def generate_batch(self, data):
        """
            Here we flatten the individual queries to run them as
            a single query in a batch
        """
        q = [cmd for query in data for cmd in query[0]]
        blobs = [blob for query in data for blob in query[1]]

        return q, blobs

    def call_response_handler(self, q, blobs, r, b):
        try:
            self.generator.response_handler(q, blobs, r, b)
        except BaseException as e:
            logger.exception(e)

    def do_batch(self, db, data):
        """
        It also provides a way for invoking a user defined function to handle the
        responses of each of the queries executed. This function can be used to process
        the responses from each of the corresponding queries in [Parallelizer](/python_sdk/parallel_exec/Parallelizer)
        It will be called once per query, and it needs to have 4 parameters:
        - requests
        - input_blobs
        - responses
        - output_blobs
        Example usage:
        ``` python
            class MyQueries(QueryGenerator):
                def process_responses(requests, input_blobs, responses, output_blobs):
                    self.requests.extend(requests)
                    self.responses.extend(responses)
            loader = ParallelLoader(self.db)
            generator = MyQueries()
            loader.ingest(generator)
        ```
        """

        q, blobs = self.generate_batch(data)

        query_time = 0
        worker_stats = {}
        if not self.dry_run:
            response_handler = None
            strict_response_validation = False
            if hasattr(self.generator, "response_handler") and callable(self.generator.response_handler):
                response_handler = self.generator.response_handler
            if hasattr(self.generator, "strict_response_validation") and isinstance(self.generator.strict_response_validation, bool):
                strict_response_validation = self.generator.strict_response_validation
            result, r, b = self.batch_command(
                q,
                blobs,
                db,
                ParallelQuery.success_statuses,
                response_handler,
                self.commands_per_query,
                self.blobs_per_query,
                strict_response_validation=strict_response_validation)
            if result == 0:
                query_time = db.get_last_query_time()
                worker_stats["suceeded_commands"] = len(q)
                worker_stats["suceeded_queries"] = len(data)
                worker_stats["objects_existed"] = sum(
                    [v['status'] == 2 for i in r for k, v in i.items()])
            elif result == 1:
                self.error_counter += 1
                worker_stats["suceeded_queries"] = 0
                worker_stats["suceeded_commands"] = 0
                worker_stats["objects_existed"] = 0
            elif result == 2:
                worker_stats["suceeded_commands"] = sum(
                    [v['status'] == 0 for i in r for k, v in i.items()])
                worker_stats["objects_existed"] = sum(
                    [v['status'] == 2 for i in r for k, v in i.items()])
                sq = 0
                for i in range(0, len(r), self.commands_per_query):
                    if all([v['status'] == 0 for j in r[i:i + self.commands_per_query] for k, v in j.items()]):
                        sq += 1
                worker_stats["suceeded_queries"] = sq
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)
        self.actual_stats.append(worker_stats)

    def worker(self, thid, generator, start, end):
        # A new connection will be created for each thread
        db = self.db.create_new_connection()

        total_batches = (end - start) // self.batchsize

        if (end - start) % self.batchsize > 0:
            total_batches += 1

        for i in range(total_batches):

            batch_start = start + i * self.batchsize
            batch_end = min(batch_start + self.batchsize, end)

            try:
                self.do_batch(db, generator[batch_start:batch_end])
            except Exception as e:
                logger.exception(e)
                logger.warning(
                    f"Worker {thid} failed to execute batch {i}: [{batch_start},{batch_end}]")
                self.error_counter += 1

            if thid == 0 and self.stats:
                self.pb.update((i + 1) / total_batches)

    def get_objects_existed(self):
        return sum([stat["objects_existed"]
                    for stat in self.actual_stats])

    def get_suceeded_queries(self):
        return sum([stat["suceeded_queries"]
                    for stat in self.actual_stats])

    def get_suceeded_commands(self):
        return sum([stat["suceeded_commands"]
                    for stat in self.actual_stats])

    def query(self, generator, batchsize=1, numthreads=4, stats=False):
        """
        This function takes as input the data to be executed in specified number of threads.
        The generator yields a tuple : (array of commands, array of blobs)
        Args:
            generator (_type_): The class that generates the queries to be executed.
            batchsize (int, optional): Nummber of queries per transaction. Defaults to 1.
            numthreads (int, optional): Number of parallel workers. Defaults to 4.
            stats (bool, optional): Show statistics at end of ingestion. Defaults to False.
        """

        use_dask = hasattr(generator, "use_dask") and generator.use_dask
        if use_dask:
            self._reset(batchsize=batchsize, numthreads=numthreads)
            self.daskmanager = DaskManager(num_workers=numthreads)

        if hasattr(self, "query_setup"):
            self.query_setup(generator)

        if use_dask:
            results, self.total_actions_time = self.daskmanager.run(
                self.__class__, self.db, generator, batchsize, stats=stats)
            self.actual_stats = []
            for result in results:
                if result is not None:
                    self.times_arr.extend(result.times_arr)
                    self.error_counter += result.error_counter
                    self.actual_stats.append(
                        {"suceeded_queries": result.suceeded_queries,
                         "suceeded_commands": result.suceeded_commands,
                         "objects_existed": result.objects_existed})
            self.total_actions = len(generator.df)

            if stats:
                self.print_stats()
        else:
            # allow subclass to do verification
            if issubclass(type(self), ParallelQuery) and hasattr(self, 'verify_generator') and callable(self.verify_generator):
                self.verify_generator(generator)
            elif len(generator) > 0:
                if isinstance(generator[0], tuple) and isinstance(generator[0][0], list):
                    # if len(generator[0]) > 0:
                    #
                    #  Not applicable to old style loaders.
                    self.commands_per_query = min(
                        len(generator[0][0]), batchsize)
                    if len(generator[0][1]):
                        self.blobs_per_query = len(generator[0][1])
                else:
                    logger.error(
                        f"Could not determine query structure from:\n{generator[0]}")
                    logger.error(type(generator[0]))
            logger.info(
                f"Commands per query = {self.commands_per_query}, Blobs per query = {self.blobs_per_query}"
            )
            self.run(generator, batchsize, numthreads, stats)

    def print_stats(self):

        times = np.array(self.times_arr)
        total_queries_exec = len(times)

        print("============ ApertureDB Parallel Query Stats ============")
        print(f"Total time (s): {self.total_actions_time}")
        print(f"Total queries executed: {total_queries_exec}")

        if total_queries_exec == 0:
            print("All queries failed!")

        else:
            mean = np.mean(times)
            std = np.std(times)
            tp = 1 / mean * self.numthreads

            print(f"Avg Query time (s): {mean}")
            print(f"Query time std: {std}")
            print(f"Avg Query Throughput (q/s): {tp}")

            i_tp = self.total_actions / self.total_actions_time
            print(
                f"Overall insertion throughput ({self.type}/s): {i_tp if self.error_counter == 0 else 'NaN'}")

            if self.error_counter > 0:
                err_perc = 100 * self.error_counter / total_queries_exec
                print(f"Total errors encountered: {self.error_counter}")
                print(f"Errors (%): {err_perc}")

        print("=========================================================")
