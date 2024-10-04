from __future__ import annotations
from typing import Any, Callable, List, Optional, Tuple
import itertools
import logging

import numpy as np

from aperturedb.ParallelQuery import ParallelQuery
from aperturedb.Connector import Connector

from aperturedb.types import Commands, Blobs

logger = logging.getLogger(__name__)

# Turn on to debug constraints ( produces a lot of output )
DEBUG_CONSTRAINTS = True

# removes blobs from a list or tuple for pretty printing


def remove_blobs(item: Any) -> Any:
    if isinstance(item, list):
        item = list(map(remove_blobs, item))
    elif isinstance(item, tuple):
        item = list(map(remove_blobs, item))
    elif isinstance(item, bytes):
        item = "*BLOB*"
    return item


def gen_execute_batch_sets(base_executor):

    #
    # execute_batch_sets - executes multiple sets of queries with optional constraints on follow on sets
    #  query_set: list of queries that get passed to ParallelLoader's execute_batch
    #  blob_set:  list of blobs ( or list of list of blobs ) that get passed to execute_batch
    #  client: Connection object
    #  success_statutes: list of return values from ApertureDB to be considered 'success'
    #  response_handler: optional function to return after an execution ( not active yet )
    #  commands_per_query : list of how many commands each query has.
    #  blobs_per_query: list of how many blobs each query has.
    #  strict_response_validation: same as execute_batch.
    #
    # if blob_set is None, or an empty list, it is ignored.
    # if blob_set is a list, it will be given to the seed query
    # if blob_set is a list of lists, each inner list will be given to the inner
    #  execution
    #
    def execute_batch_sets(client, query_set, blob_set, success_statuses: list[int] = [0],
                           response_handler: Optional[Callable] = None, commands_per_query: list[int] = -1,
                           blobs_per_query: list[int] = -1,
                           strict_response_validation: bool = False, cmd_index: int = None):

        logger.info("Execute Batch Sets = Batch Size {0}  Comands Per Query {1} Blobs Per Query {2}".format(
            len(query_set), commands_per_query, blobs_per_query))

        batch_size = len(query_set)

        # test query set
        first_element = query_set[0]
        if not isinstance(first_element, list):
            logger.error("First Element not a list: {first_element}")
            raise Exception("Query set must be a list of lists")
        set_total = len(first_element)

        # Check if blobs are a simple array or nested array of blobs
        per_set_blobs = isinstance(blob_set, list) and len(
            blob_set) > 0 and isinstance(blob_set[0], list)

        # verify layout if a complex set
        if per_set_blobs:
            first_element_blobs = blob_set[0]

            if len(first_element_blobs) == 0 or len(first_element_blobs) != set_total:
                # user has confused blob format for sure.
                logger.error("Malformed blobs for first element. Blob return from your loader "
                             "should be [query_blobs] where query_blobs = [ first_cmd_list, second_cmd_list, ... ] ")
                raise Exception(
                    "Malformed blobs input. Expected First element to have a list of blobs for each set.")

            first_query_blobs = first_element_blobs[0]
            # If someone is looking for info logging from PQS, it is likely that blobs are not being set properly.
            #  The wrapping of blobs in general can be confusing. Best suggestion is looking at a loader.
            logger.info("Blobs for first set = " +
                        str(remove_blobs(first_element_blobs)))
            logger.info("First Blob for first set = " +
                        str(remove_blobs(first_query_blobs)))
            if not isinstance(first_query_blobs, list):
                logger.error(
                    "Expected a list of lists for the first element's blob sets")
                expected_list = remove_blobs(first_query_blobs)
                logger.error(
                    f"Blob input for set is : {expected_list}, but should be a list")
                raise Exception(
                    "Could not determine blob strategy; first batch element doesn't have a list of blobs for the first query set. Are you missing a list wrapping in the CVS parser?")

            for set_i in range(len(first_element_blobs)):
                expected_set = first_element_blobs[set_i]
                if not isinstance(expected_set, list):
                    without_blobs = remove_blobs(expected_set)
                    logger.error(
                        f"Blob input for set {set_i} for first item is : {without_blobs}, but should be a list")
                    raise Exception(
                        f"Could not determine blob strategy; first batch element doesn't have a list of blobs for set {set_i}")
                if len(expected_set) != blobs_per_query[set_i]:
                    without_blobs = remove_blobs(expected_set)
                    logger.error(
                        f"Blob input for set {set_i} for first item is : {without_blobs}, but expecting a length of {blobs_per_query[set_i]}")
                    raise Exception(
                        f"Could not executed blob strategy; first batch element's list length doesn't match blob_per_query for the set {set_i} ( {len(expected_set)} != {blobs_per_query[set_i]} )")

        # define method for extracting the blobs
        def blob_filter(all_blobs, strike_list, set_nm): return all_blobs

        # set blob filter based on how blobs are passed to function
        if per_set_blobs:
            # if each query_set has blobs, we must extract out the n-th element in each blob set

            def set_blob_filter(all_blobs, strike_list, set_nm):
                # the list comprehension pulls out the blob set for the requested set
                # the blob set is then flattened as the query expects a flat array using blobs_per_query as the iterator
                # the flat list is them zipped with the strike list, which determines which blobs are unused
                # the filter checks if the blob is to be struck
                # the map pulls the remaining blobs out

                return list(map(lambda pair: pair[0],
                            filter(lambda pair: pair[1] is not True,
                                   itertools.zip_longest(
                                itertools.chain(*[blob_set[set_nm]
                                                for blob_set in all_blobs]),
                                strike_list)
                )
                ))
            blob_filter = set_blob_filter
        else:
            def first_only_blobs(all_blobs, strike_list, set_nm):
                if set_nm == 0:
                    # same as above, without the list comprehension and flattening.
                    return list(map(lambda pair: pair[0], filter(lambda pair: pair[1] is not False,   itertools.zip_longest(all_blobs, strike_list))))
                else:
                    return []
            blob_filter = first_only_blobs

        # start execution
        stored_results = {}
        db_results = []
        db_blobs = None
        for i in range(0, set_total):

            # total number of blobs in this set
            # passing in [] allows for no strikes, so maximum size.
            blobs_this_set = len(blob_filter(blob_set, [], i))
            expected_blobs = blobs_per_query[i] * batch_size
            logger.info(
                f"Set {i}: Commands per query = {commands_per_query[i]}, Blobs per query = {blobs_per_query[i]}"
            )
            if blobs_this_set != expected_blobs:
                logger.error(
                    f"Set {i}: Expected {expected_blobs} blobs, but filter is returning {blobs_this_set}"
                )

            # now we determine if the executing set has a constraint
            # in the head

            # allowed layouts for commands other than the seed command
            # { "cmd" : {} } -> standard single command
            # [{ "cmd1": {} },{ "cmd2" : {} }] -> standard multiple command
            # [{ constraints } , { "cmd" : {} }] -> constraint with a single command
            # [{ constraints } , [{"cmd1" : {} }, {"cmd2": {} }]] -> constraint with multiple command

            known_constraint_keys = ["results", "apply"]
            constraints = None
            # define filter to optionally remove constraints when passing to executor
            def query_filter(entity, entity_results): return entity[i]

            if i != 0 and isinstance(first_element[i], list) and len(first_element[i]) > 0:
                set_component = first_element[i][0]
                if not isinstance(set_component, list) and any([k in known_constraint_keys for k in set_component.keys()]):
                    constraints = set_component

            if constraints is not None:
                # operations for constraints
                def check_apply_constraint(test_op, item_a, item_b):
                    if DEBUG_CONSTRAINTS:
                        logger.debug(
                            f"check_apply_constraint {test_op} {item_a} {item_b}")
                    if test_op == "==":
                        return item_a == item_b
                    elif test_op == ">":
                        return item_a > item_b
                    elif test_op == "<":
                        return item_a < item_b
                    elif test_op == "!=":
                        return item_a != item_b
                    else:
                        raise Exception(
                            f"Unhandled constraint {test_op} in check_apply_constraint")

                known_constraint_keywords = ['results', 'apply']
                # function called for each row in the set to decide if a query is executed

                def constraint_filter(single_line, single_results):
                    current_constraints = single_line[i][0]

                    if DEBUG_CONSTRAINTS:
                        logger.debug(f"constraint = {current_constraints}")
                        logger.debug(f"results = {single_results}")

                    if 'results' in current_constraints:
                        result_constraints = current_constraints['results']
                        passed_all_constraints = True
                        for result_number in result_constraints:

                            if not isinstance(result_number, int):
                                raise Exception("Keys for result constraints must be numbers: "
                                                f"{result_number} is {type(result_number)}")

                            if len(single_results) < result_number or single_results[result_number] is None:
                                # in theory here we have two possibilities: a user can have a correctly formed constraint which didn't execute by design
                                # ( which is what process here )
                                #  or they can have a mis-configured constraint, which is harder to detect.
                                if DEBUG_CONSTRAINTS:
                                    logger.debug(
                                        f"Failed applying constraints, requested results from operation {result_number}, but none existed ( Query didn't run )")
                                passed_all_constraints = False
                                break

                            # query this constraint references
                            prev_query = single_line[result_number]

                            if DEBUG_CONSTRAINTS:
                                logger.debug(
                                    f"prev_query = {single_line[result_number]}")

                            if isinstance(prev_query, dict):
                                prev_query_cmd = [
                                    cmd for cmd in prev_query.keys()][0]
                            elif isinstance(prev_query, list) and isinstance(prev_query[0], dict) \
                                    and all(map(lambda k: k in known_constraint_keywords, prev_query[0].keys()))  \
                                    and isinstance(prev_query[1], dict):
                                prev_query_cmd = [
                                    cmd for cmd in prev_query[1].keys()][0]
                            else:
                                raise Exception(
                                    f"Contraints only implemented with with single queries; query {result_number} not single item.")

                            target_results = single_results[result_number][prev_query_cmd]
                            target_constraints = result_constraints[result_number]
                            for result_item in target_constraints:
                                test = target_constraints[result_item]
                                if not result_item in target_results:
                                    logger.debug(
                                        f"failed results = {target_results}")
                                    raise Exception(
                                        f"Failed applying constraints, requested result '{result_item}' from operation {result_number}, but none exited")
                                # if constraint passes, apply:
                                if not check_apply_constraint(test[0], target_results[result_item], test[1]):
                                    passed_all_constraints = False
                                    break

                        if passed_all_constraints:
                            return single_line[i][1]
                        else:
                            return None
                    elif 'apply' in current_constraints:
                        # apply means run the line
                        return single_line[i][1]
                    else:
                        raise Exception(
                            f"incorrectly formatted constraint; no known conatraint action")

                query_filter = constraint_filter

            local_success_statuses = [0, 2]

            # queries are by row first, so we run query_filter on each query
            # we pass the entire row's data, then we retrieve all of the stored results for that row

            queries = [query_filter(query_set[row_num], [stored_results[res_grp][row_num] for res_grp in stored_results])
                       for row_num in range(0, len(query_set))]

            # finally, we remove queries that were reduced to None so the base executor doesn't have to deal with them
            # and unwrap multiple commands per element into a flat list; map allows one command to process either type
            #  by wrapping all in a list.
            executable_queries = list(itertools.chain(*
                                                      map(lambda q: q if isinstance(q, list) else [q],
                                                          filter(lambda q: q is not None, queries))
                                                      ))

            # produce strike list based on queries that are none
            blob_strike_list = list(map(lambda q: q is None, queries))

            # filter out struck blobs
            used_blobs = list(filter(lambda b: b is not None,
                                     blob_filter(blob_set, blob_strike_list, i)))

            if len(executable_queries) > 0:
                result_code, db_results, db_blobs = base_executor(client, executable_queries, used_blobs,
                                                                  local_success_statuses,
                                                                  None,
                                                                  commands_per_query[i],
                                                                  blobs_per_query[i],
                                                                  strict_response_validation=strict_response_validation,
                                                                  cmd_index=cmd_index)
                if response_handler != None and client.last_query_ok():
                    def map_to_set(query, query_blobs, resp, resp_blobs):
                        response_handler(
                            i, query, query_blobs, resp, resp_blobs)
                    try:
                        ParallelQuery.map_response_to_handler(map_to_set,
                                                              executable_queries, used_blobs, db_results, db_blobs, commands_per_query[i], blobs_per_query[i])
                    except BaseException as e:
                        logger.exception(e)
                        if strict_response_validation:
                            raise e
            else:
                logger.info(
                    f"Skipped executing set {i}, no executable queries")

            if result_code == 1:
                logger.error(
                    f"Ran into error on set {i} in ParallQuerySet, unable to continue")
                return 1, db_results, db_blobs

            # expand results so queries that didn't run show up as a None
            results_off = 0

            def insert_empty_results(result_value):
                nonlocal results_off
                if result_value is None:
                    return None
                else:
                    results_off += 1
                    return db_results[(results_off - 1)]

            stored_results[i] = [insert_empty_results(q) for q in queries]

        return 0, db_results, db_blobs  # end execute_batch_sets
    return execute_batch_sets


class ParallelQuerySet(ParallelQuery):
    """
    **Parallel and Batch Set Multi-Querier for ApertureDB**
    This class provides the mechanism to run multiple queries over a single csv.
    Per-query actions are done by ParallelQuery.

    Args:
        client (Connector): The ApertureDB Connector
        dry_run (bool, optional): If True, no queries are executed. Defaults to False.
    """

    def __init__(self, client: Connector, dry_run: bool = False):

        super().__init__(client, dry_run)

        self.base_batch_command = self.batch_command

    def verify_generator(self, generator) -> bool:
        # first level should be grouping of commands
        # first cmd should have a list of query sets
        if isinstance(generator[0], list) or isinstance(generator[0], tuple):
            cmd = generator[0]
            if isinstance(generator[0][0], list):
                return True

        logger.error(
            f"Could not determine query structure from:\n{generator[0]}")
        logger.error(type(generator[0]))
        return False

    def do_batch(self, client: Connector, batch_start: int,  data: List[Tuple[Commands, Blobs]]) -> None:
        """
        This is an override of ParallelQuery.do_batch.

        This is the per-worker function which is the entry-point to a unit of work.

        Args:
            client (Connector): The ApertureDB Connector
            data (List[Tuple[Query, Blobs]]): A list of tuples, each containing a list of commands and a list of blobs
        """
        if not hasattr(self.generator, "commands_per_query"):
            raise Exception(
                "Generator must specify commands per query for ParallelQuerySet")
        if not hasattr(self.generator, "blobs_per_query"):
            raise Exception(
                "Generator must specify blobs per query for ParallelQuerySet")
        self.commands_per_query = self.generator.commands_per_query
        self.blobs_per_query = self.generator.blobs_per_query
        set_response_handler = None
        self.batch_command = gen_execute_batch_sets(
            self.base_batch_command)

        ParallelQuery.do_batch(self, client, batch_start, data)

    def print_stats(self) -> None:

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
            std = np.std(times)
            tp = 0 if mean == 0 else 1 / mean * self.numthreads

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
