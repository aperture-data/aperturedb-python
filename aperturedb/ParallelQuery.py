from aperturedb import Parallelizer
import numpy as np
import json
import logging
import math

logger = logging.getLogger(__name__)


class ParallelQuery(Parallelizer.Parallelizer):
    """
    **Parallel and Batch Querier for ApertureDB**
    This class provides the abstraction for partitioning data into batches,
    so that they may be processed using different threads.
    """

    def __init__(self, db, dry_run=False):

        super().__init__()

        self.db = db.create_new_connection()

        self.dry_run = dry_run

        self.type = "query"

        self.responses = []

        self.commands_per_query = 1
        self.blobs_per_query = 0

    def generate_batch(self, data):
        """
            Here we flatten the individual queries to run them as
            a single query in a batch
        """
        q     = [cmd for query in data for cmd in query[0]]
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
        the responses from each of the corresponding queries in :class:`~aperturedb.Parallelizer.Parallelizer`.
        It will be called once per query, and it needs to have 4 parameters:

        - requests

        - input_blobs

        - responses

        - output_blobs

        Example usage:

        .. code-block:: python

            class MyQueries(QueryGenerator):
                def process_responses(requests, input_blobs, responses, output_blobs):
                    self.requests.extend(requests)
                    self.responses.extend(responses)

            loader = ParallelLoader(self.db)
            generator = MyQueries()
            loader.ingest(generator)

        """

        q, blobs = self.generate_batch(data)

        query_time = 0

        if not self.dry_run:
            r, b = db.query(q, blobs)
            logger.info(f"Query={q}")
            logger.info(f"Response={r}")

            if db.last_query_ok():
                if hasattr(self.generator, "response_handler") and callable(self.generator.response_handler):
                    logger.info(
                        f"Response_handler={self.generator.response_handler}")
                    # We could potentially always call this handler function
                    # and let the user deal with the error cases.
                    blobs_returned = 0
                    for i in range(math.ceil(len(q) / self.commands_per_query)):
                        start = i * self.commands_per_query
                        end = start + self.commands_per_query

                        blobs_start = i * self.blobs_per_query
                        blobs_end = blobs_start + self.blobs_per_query

                        b_count = 0
                        for req, resp in zip(q[start:end], r[start:end]):
                            for k in req:
                                # Ref to https://docs.aperturedata.io/parameters/blobs.html
                                blobs_where_default_true = \
                                    k in ["FindImage", "FindBlob", "FindBlobs"] and (
                                        "blobs" not in req[k] or req[k]["blobs"])
                                blobs_where_default_false = \
                                    k in [
                                        "FindDescriptors", "FindBoundingBoxes"] and "blobs" in req[k] and req[k]["blobs"]
                                if blobs_where_default_true or blobs_where_default_false:
                                    count = resp[k]["returned"]
                                    b_count += count

                        self.call_response_handler(
                            q[start:end],
                            blobs[blobs_start:blobs_end],
                            r[start:end],
                            b[blobs_returned:blobs_returned + b_count] if len(b) < blobs_returned + b_count else None)
                        blobs_returned += b_count

            else:
                # Transaction failed entirely.
                logger.error(f"Failed query = {q} with response = {r}")
                self.error_counter += 1

            if isinstance(r, list) and not all([v['status'] == 0 for i in r for k, v in i.items()]):
                logger.warning(
                    f"Partial errors:\r\n{json.dumps(q)}\r\n{json.dumps(r)}")
            query_time = db.get_last_query_time()
        else:
            query_time = 1

        # append is thread-safe
        self.times_arr.append(query_time)

    def worker(self, thid, generator, start, end):

        # A new connection will be created for each thread
        db = self.db.create_new_connection()

        total_batches = (end - start) // self.batchsize

        if (end - start) % self.batchsize > 0:
            total_batches += 1

        for i in range(total_batches):

            batch_start = start + i * self.batchsize
            batch_end   = min(batch_start + self.batchsize, end)

            try:
                self.do_batch(db, generator[batch_start:batch_end])
            except Exception as e:
                logger.exception(e)
                self.error_counter += 1

            if thid == 0 and self.stats:
                self.pb.update((i + 1) / total_batches)

    def query(self, generator, batchsize=1, numthreads=4, stats=False):
        """
        This function takes as input the data to be executed in specified number of threads.
        The generator yields a tuple : (array of commands, array of blobs)

        Args:
            generator (_type_): _description_
            batchsize (int, optional): _description_. Defaults to 1.
            numthreads (int, optional): _description_. Defaults to 4.
            stats (bool, optional): _description_. Defaults to False.
        """
        if len(generator) > 0:
            if isinstance(generator[0], tuple) and isinstance(generator[0][0], list):
                # if len(generator[0]) > 0:
                # Not applicable to old style loaders.
                self.commands_per_query = min(len(generator[0][0]), batchsize)
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
        print("Total time (s):", self.total_actions_time)
        print("Total queries executed:", total_queries_exec)

        if total_queries_exec == 0:
            print("All queries failed!")

        else:
            print("Avg Query time (s):", np.mean(times))
            print("Query time std:", np.std(times))
            print("Avg Query Throughput (q/s)):",
                  1 / np.mean(times) * self.numthreads)

            msg = "(" + self.type + "/s):"
            print("Overall throughput", msg,
                  self.total_actions / self.total_actions_time)

            if self.error_counter > 0:
                print("Total errors encountered:", self.error_counter)
                print("Errors (%):", 100 *
                      self.error_counter / total_queries_exec)

        print("=========================================================")
