import argparse
import time

import dbinfo

from aperturedb import Connector, EntityLoader

def main(params):

    db = Connector.Connector(params.db_host, params.db_port)

    print("Creating Generator from CSV...")
    generator = EntityLoader.EntityGeneratorCSV(params.in_csv_file)
    print("Generator done.")

    loader = EntityLoader.EntityLoader(db)
    loader.ingest(generator, batchsize=params.batchsize,
                             numthreads=params.numthreads,
                             stats=True)

def get_args():
    obj = argparse.ArgumentParser()

    # Database config
    obj.add_argument('-db_host', type=str, default=dbinfo.DB_HOST)
    obj.add_argument('-db_port', type=int, default=dbinfo.DB_PORT)

    # Run Config
    obj.add_argument('-numthreads', type=int, default=32)
    obj.add_argument('-batchsize',  type=int, default=100)

    # Input CSV
    obj.add_argument('-in_csv_file', type=str,
                     default="input/persons.adb.csv")

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
