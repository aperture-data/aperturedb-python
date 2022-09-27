import dask
from dask import dataframe
import os
from aperturedb import EntityDataCSV
from dask.distributed import Client, SSHCluster, performance_report

if __name__ == "__main__":
    batchsize = 4000
    numthreads = 14

    # cluster = SSHCluster(
    #     [ "www.coco2.on.aperturedata.io"],
    #     worker_options = {"n_workers": numthreads//2},
    #     connect_options = { "username": "ubuntu"}
    # )
    client = Client("localhost:8786")
    dask.config.set(scheduler="distributed")

    FILENAME = os.path.join('see2.out')

    ratings = dataframe.read_csv(
        FILENAME, blocksize=os.path.getsize(FILENAME) // numthreads)

    def process(df):
        import dbinfo
        from aperturedb.ParallelLoader import ParallelLoader
        db = dbinfo.create_connector()
        loader = ParallelLoader(db)
        count = 0

        for i in range(0, len(df), batchsize):
            end = min(i + batchsize, len(df))
            batch = df[i:end]
            data = EntityDataCSV.EntityDataCSV(filename="", df=batch)
            loader.ingest(data, batchsize=len(batch), numthreads=1)
            count += 1

        print(f"len(df) = {len(df)}, count = {count}")

    with performance_report(filename="ingestion_report.html"):
        ratings.map_partitions(process).compute()
