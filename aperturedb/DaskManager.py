"""
**Module providing the DaskManager.**

This module provides the `DaskManager` class responsible for setting up a
local Dask cluster and assigning parts of data to each worker to perform
parallel distributed processing.
"""
from __future__ import annotations
import logging
from threading import Lock
import time
from types import SimpleNamespace
import dask
from dask.distributed import Client, LocalCluster, progress
from aperturedb.Connector import Connector

import multiprocessing as mp

from aperturedb.Stats import Stats

dask.config.set({"dataframe.convert-string": False})

logger = logging.getLogger(__name__)


class DaskManager:
    """
    **Class responsible for setting up a local cluster and assigning parts
    of data to each worker**
    """

    def __init__(self, num_workers: int = -1):
        self.__num_workers = num_workers
        # The -1 magic number is to use as many 90% of the cores (1 worker per core).
        # This can be overridden by the user.
        # Create a pool of workers.
        # TODO: see if the same pool can be reused for multiple tasks.
        workers = self.__num_workers if self.__num_workers != \
            -1 else int(0.9 * mp.cpu_count())

        self._cluster = LocalCluster(n_workers=workers)
        self._cluster.shutdown_on_close = False
        self._client = Client(self._cluster)
        dask.config.set(scheduler="distributed")

    def __del__(self):
        logger.info(".......Shutting cluster.........")
        self._client.close()
        self._cluster.close()

    def run(self, QueryClass: type[ParallelQuery], client: Connector, generator, batchsize, stats, dry_run=False):
        def process(df, host, port, use_ssl, ca_cert, verify_hostname, session, connector_type, dry_run):
            metrics = Stats()
            # Dask reads data in partitions, and the first partition is of 2 rows, with all
            # values as 'foo'. This is for sampling the column names and types. Should not process
            # those rows.
            if len(df) == 2:
                if (df.iloc[0, 0] == "a" and df.isna().iloc[1, 0]) or df.iloc[0, 0] == "foo":
                    return
            count = 0
            try:
                shared_data = SimpleNamespace()
                shared_data.session = session
                shared_data.lock = Lock()
                client = connector_type(
                    host=host, port=port,
                    use_ssl=use_ssl,
                    ca_cert=ca_cert,
                    verify_hostname=verify_hostname,
                    shared_data=shared_data)
            except Exception as e:
                logger.exception(e)
                raise
            #from aperturedb.ParallelLoader import ParallelLoader
            import inspect
            sig = inspect.signature(QueryClass.__init__)
            if "dry_run" in sig.parameters or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                loader = QueryClass(client, dry_run=dry_run)
            else:
                loader = QueryClass(client)
                loader.dry_run = dry_run
            for i in range(0, len(df), batchsize):
                end = min(i + batchsize, len(df))
                slice = df[i:end]
                import copy
                data = copy.copy(generator)
                data.df = slice

                loader.query(generator=data, batchsize=len(
                    slice), numthreads=1, stats=False)
                count += 1
                metrics.times_arr.extend(loader.times_arr)
                metrics.error_counter += loader.error_counter
                metrics.succeeded_queries += loader.get_succeeded_queries()
                metrics.succeeded_commands += loader.get_succeeded_commands()

            return metrics

        start_time = time.time()
        # Connector cannot be serialized across processes,
        # so we pass session and host/port information instead.

        if not hasattr(generator, "df"):
            raise ValueError("Dask mode requires a generator with a 'df' attribute.")

        if not hasattr(generator.df, "map_partitions"):
            # If generator.df is a Pandas DataFrame, convert to Dask DataFrame
            # Alternatively, if it has a filename, we could read it directly with dask.
            import dask.dataframe as dd
            import os
            import multiprocessing as mp
            PARTITIONS_PER_CORE = 10
            CORES_USED_FOR_PARALLELIZATION = 0.9

            if hasattr(generator, "filename") and generator.filename:
                cores_used = max(1, int(
                    CORES_USED_FOR_PARALLELIZATION * mp.cpu_count()))
                blocksize = os.path.getsize(
                    generator.filename) // (cores_used * PARTITIONS_PER_CORE)
                if blocksize == 0:
                    cpus = mp.cpu_count()
                    raise Exception(
                        f"CSV file too small to be read in parallel. Use normal mode. cpus: {cpus}")
                dask_df = dd.read_csv(
                    generator.filename,
                    blocksize=blocksize)
            else:
                dask_df = dd.from_pandas(
                    generator.df, npartitions=self.__num_workers * 2 if self.__num_workers > 0 else mp.cpu_count())
        else:
            dask_df = generator.df

        computation = dask_df.map_partitions(
            process,
            client.host,
            client.port,
            client.use_ssl,
            client.config.ca_cert,
            client.config.verify_hostname,
            client.shared_data.session,
            type(client),
            dry_run)
        computation = computation.persist()
        if stats:
            progress(computation)
        results = computation.compute()

        return results, time.time() - start_time
