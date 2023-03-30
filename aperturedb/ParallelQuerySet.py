from __future__ import annotations
from typing import Callable
from aperturedb.ParallelQuery import ParallelQuery
import logging
import numpy as np



logger = logging.getLogger(__name__)


# if blob_set is None, or an empty list, it is ignored.
# if blob_set is a list, it will be given to the seed query
# if blob_set is a list of lists, each inner list will be given to the inner
#  execution

def gen_execute_batch_sets( base_executor, per_batch_response_handler: Callable = None ):

    def execute_batch_sets( query_set, blob_set, db, success_statuses: list[int] = [0],
            response_handler: Callable = None, commands_per_query: list[int] = -1, blobs_per_query: list[int] = -1):


        # expand results to sparse
        def expand_results( orig, results ):

            return results
        print("Execute Batch Sets = cpq {0} bpq {1}".format(commands_per_query,blobs_per_query))
        first_element = query_set[0]

        if not isinstance( first_element, list ):
            print(first_element)
            raise Exception( "Query set must be a list of lists" )
        set_total = len(first_element)

        # blobs will be passed to each set only
        per_set_blobs = isinstance( blob_set, list ) and len(blob_set) > 0 and isinstance( blob_set[0], list )

        # method for extracting the blobs
        blob_filter = lambda all_blobs,set_nm:all_blobs

        if per_set_blobs:
            def set_blob_filter(all_blobs,set_nm):
                return [blob_set[set_nm] for blob_set in all_blobs]
            blob_filter = set_blob_filter
        else:
            def first_only_blobs(all_blobs,set_nm):
                if set_nm == 0 :
                    return all_blobs
                else:
                    return []
            blob_filter = first_only_blobs

        stored_results = {}
        db_results = None
        db_blobs = None
        for i in range(0,set_total):
            execute = True

            # now we determine if the executing set has a constraint
            # in the head

            # allowed layouts for commands other than the seed command
            # { "cmd" : {} } -> standard single command
            # [{ "cmd1": {}, "cmd2} : {}] -> standard multiple command
            # [{ "constraint" : {} , { "cmd" : {} }] -> constraint with a single command
            # [{ "constraints: {} , [{"cmd1" : {} }, {"cmd2": {} }]] -> constraint with multiple command

            known_constraint_keys = [ "results" , "apply" ]
            constraints = None
            # filter to optionally remove constraints when passing to executor
            query_filter = lambda entity,entity_results: entity[i]

            if i != 0 and isinstance( first_element[i],list) and len(first_element[i]) > 0:
                print(f"item {i} is a list = {first_element[i]}")
                set_component = first_element[i][0]
                if not isinstance(set_component,list) and any( [ k in known_constraint_keys for k in set_component.keys() ] ):
                    constraints = set_component


            if constraints is not None:
                def check_apply_constraint( test_op, item_a,item_b):
                    print(f"check_apply_constraint {test_op} {item_a} {item_b}")
                    return False
                def constraint_filter(single_line,single_results):
                    single_constraints = single_line[i][0]

                    print(f"SC is {single_constraints}")
                    print(f"RC is {single_results}")
                    if 'results' in single_constraints:
                        result_constraints = single_constraints['results']
                        for result_number in result_constraints:

                            if len(single_results) < result_number or single_results[result_number] is None:
                                raise Exception(f"Failed applying constraints, requested results from operation {result_number}, but none existed")

                            # query this constraint references
                            prev_query = single_line[result_number]

                            if not isinstance(prev_query,dict):
                                raise Exception("Contraints only implemented with with single queries; query {result_number} not single item.")
                            prev_query_cmd = [ cmd for cmd in prev_query.keys() ][0]

                            target_results = single_results[result_number][prev_query_cmd]
                            target_constraints = result_constraints[result_number]
                            for result_item in target_constraints:
                                test = target_constraints[result_item]
                                if not result_item in target_results:
                                    print("SR = ",target_results)
                                    raise Exception(f"Failed applying constraints, requested result '{result_item}' from operation {result_number}, but none exited")
                                # if constraint passes, apply:
                                if check_apply_constraint( test[0], target_results[result_item], test[1] ):
                                    return single_line[i][1]
                                else:
                                    return None
                    elif 'apply' in single_constraints:
                        # apply means run the line
                        return single_line[i][1]
                    else:
                        raise Exception(f"incorrectly formatted constraint; no known conatraint action")

                query_filter = constraint_filter


            execute = True
            local_success_statuses = [ 0 , 2 ]
            #rq = [ query_filter(entity) for entity in query_set ]
            #print("Running = ",rq)
            #result_code,db_results,db_blobs = base_executor([ query_filter(entity,entity_results) for entity,entity_results in zip(query_set,],
            print("len qs = ",len(query_set))
            print("qs = ",query_set)
            print("enums = ",[enum for enum in range(0,len(query_set))])
            [print(f"#{enum} = {query_set[enum]}\n") for enum in range(0,len(query_set))]
            print(f"= ",[resg for resg in stored_results])
            [print(f"#{enum} = ",[stored_results[resg][enum] for resg in stored_results])  for enum in range(0,len(query_set))]
            queries = [ query_filter(query_set[enum],[stored_results[resg][enum] for resg in stored_results])
                        for enum in range(0,len(query_set))]
            executable_queries = list(filter( lambda q: q is not None, queries ))

            if len(executable_queries) > 0:
                result_code,db_results,db_blobs = base_executor( queries,
                                                                blob_filter(blob_set,i), db,local_success_statuses,
                                                                None,commands_per_query[i],blobs_per_query[i])
            else:
                logger.info(f"Skipped executing set {i}, no executable queries")
            # expand results
            off = 0
            empty_off = 0
            def insert_empty_results(result_value):
                nonlocal off
                nonlocal empty_off
                if result_value is None:
                    empty_off += 1
                    return None
                else:
                    off += 1
                    return db_results[(off-1)+empty_off]

            stored_results[i] = [ insert_empty_results( q ) for q in queries ]
            if result_code == 1:
                logger.error(f"Ran into error on set {i} in ParallQuerySet, unable to continue")
                return 1,db_results,db_blobs

        return 0,db_results,db_blobs # end execute_batch_sets
    return execute_batch_sets


class ParallelQuerySet(ParallelQuery):
    """
    **Parallel and Batch Querier for ApertureDB**
    This class provides the abstraction for partitioning data into batches,
    so that they may be processed using different threads.
    """


    def __init__(self, db, dry_run=False):

        super().__init__(db,dry_run)

        self.base_batch_command = self.batch_command


        # set self.blobs_per_query
        # self.commands_per_query
        #if hasattr(self.generator, "response_handler") and callable(self.generator.response_handler):
        #    response_handler = self.generator.response_handler



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
        if not hasattr(self.generator, "commands_per_query"):
            raise Exception("Generator must specify commands per query for ParallelQuerySet")
        if not hasattr(self.generator, "blobs_per_query"):
            raise Exception("Generator must specify blobs per query for ParallelQuerySet")
        self.commands_per_query = self.generator.commands_per_query
        self.blobs_per_query = self.generator.blobs_per_query
        set_response_handler = None
        if hasattr(self.generator, "set_response_handler") and callable(self.generator.set_response_handler):
            set_response_handler = self.generator.set_response_handler
        self.batch_command = gen_execute_batch_sets( self.base_batch_command, set_response_handler )

        ParallelQuery.do_batch(self,db,data)

    def print_stats(self):

        times = np.array(self.times_arr)
        total_queries_exec = len(times)

        # Todo - implement per-set data.

        print("============ ApertureDB Parallel Query Set Stats ============")
        print(f"Total time (s): {self.total_actions_time}")
        print(f"Total queries executed: {total_queries_exec}")

        if total_queries_exec == 0:
            print("All queries failed!")

        else:
            mean = np.mean(times)
            std  = np.std(times)
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
