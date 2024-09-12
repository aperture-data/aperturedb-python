import dask
from dask import dataframe
import os
from aperturedb import EntityDataCSV
from dask.distributed import Client, LocalCluster


if __name__ == '__main__':
    batchsize = 2000
    numthreads = 8

    cluster = LocalCluster(n_workers=numthreads)
    client = Client(cluster)
    dask.config.set(scheduler="distributed")

    FILENAME = os.path.join(os.path.dirname(__file__), 'see2.out')

    ratings = dataframe.read_csv(
        FILENAME, blocksize=os.path.getsize(FILENAME) // numthreads)

    def process(df):
        from aperturedb.CommonLibrary import create_connector
        from aperturedb.ParallelLoader import ParallelLoader
        client = create_connector()
        loader = ParallelLoader(client)
        count = 0

        for i in range(0, len(df), batchsize):
            end = min(i + batchsize, len(df))
            batch = df[i:end]
            data = EntityDataCSV.EntityDataCSV(filename="", df=batch)
            loader.ingest(data, batchsize=len(batch), numthreads=1)
            count += 1

        print(f"len(df) = {len(df)}, count = {count}")

    ratings.map_partitions(process).compute()
