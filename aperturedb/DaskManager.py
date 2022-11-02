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

logger = logging.getLogger(__name__)


class DaskManager:
    def __init__(self, num_workers: int = -1):
        self.__num_workers = num_workers

    def run(self, db: Connector, generator, batchsize, stats):
        def process(df, host, port, session):
            metrics = Stats()
            # Dask reads data in partitions, and the first partition is of 2 rows, with all
            # values as 'foo'. This is for sampling the column names and types. Should not process
            # those rows.
            if len(df) == 2:
                if df.iloc[0, 0] == "foo":
                    return
            count = 0
            try:
                shared_data = SimpleNamespace()
                shared_data.session = session
                shared_data.lock = Lock()
                db = Connector(host=host, port=port, shared_data=shared_data)
            except Exception as e:
                logger.exception(e)
            from aperturedb.ParallelLoader import ParallelLoader
            loader = ParallelLoader(db)
            for i in range(0, len(df), batchsize):
                end = min(i + batchsize, len(df))
                slice = df[i:end]
                data = generator.__class__(filename="", df=slice)
                loader.ingest(generator=data, batchsize=len(
                    slice), numthreads=1, stats=False)
                count += 1
                metrics.times_arr.extend(loader.times_arr)
                metrics.error_counter += loader.error_counter

            return metrics

        # The -1 magic number is to use as many 90% of the cores (1 worker per core).
        # This can be overridden by the user.
        # Create a pool of workers.
        # TODO: see if the same pool can be reused for multiple tasks.
        workers = self.__num_workers if self.__num_workers != \
            -1 else int(0.9 * mp.cpu_count())
        with LocalCluster(n_workers=workers) as cluster, Client(cluster) as client:
            dask.config.set(scheduler="distributed")
            start_time = time.time()
            # Passing DB as an argument to function is not supported by Dask,
            # so we pass session and host/port instead.
            computation = generator.df.map_partitions(
                process,
                db.host,
                db.port,
                db.shared_data.session)
            computation = computation.persist()
            if stats:
                progress(computation)
            results = computation.compute()
        return results, time.time() - start_time
