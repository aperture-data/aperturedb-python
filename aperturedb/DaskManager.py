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

    def run(self, QueryClass: type[ParallelQuery], client: Connector, generator, batchsize, stats):
        def process(df, host, port, use_ssl, session, connnector_type):
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
                client = connnector_type(host=host, port=port,
                                         use_ssl=use_ssl, shared_data=shared_data)
            except Exception as e:
                logger.exception(e)
            #from aperturedb.ParallelLoader import ParallelLoader
            loader = QueryClass(client)
            for i in range(0, len(df), batchsize):
                end = min(i + batchsize, len(df))
                slice = df[i:end]
                data = generator.__class__(
                    filename=generator.filename,
                    df=slice,
                    blobs_relative_to_csv=generator.blobs_relative_to_csv)

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
        computation = generator.df.map_partitions(
            process,
            client.host,
            client.port,
            client.use_ssl,
            client.shared_data.session,
            type(client))
        computation = computation.persist()
        if stats:
            progress(computation)
        results = computation.compute()

        return results, time.time() - start_time
