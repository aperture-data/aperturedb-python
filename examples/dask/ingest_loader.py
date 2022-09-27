import os
from aperturedb.EntityDataCSV import EntityDataCSV
from aperturedb.ParallelLoader import ParallelLoader
import dbinfo

db = dbinfo.create_connector()

data = EntityDataCSV(filename=os.path.join(
    os.path.dirname(__file__), 'see2.out'))
loader = ParallelLoader(db=db)
loader.ingest(generator=data, batchsize=2000, numthreads=8, stats=True)
