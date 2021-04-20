import argparse
import random
from datetime import datetime

import pandas as pd

from itertools import product

def generate_person_csv():

	names    = ["James", "Luis", "Sole", "Maria", "Tom", "Xavi", "Dimitris", "King"]
	lastname = ["Ramirez", "Berlusconi", "Copola", "Tomson", "Ferro"]
	persons  = list(product(names, lastname))

	entity   = [ "Person"                     for x in range(len(persons)) ]
	ids		 = [int(1000000* random.random()) for i in range(len(persons))]
	age		 = [int(100* random.random())     for i in range(len(persons))]
	height   = [float(200* random.random())   for i in range(len(persons))]
	dog      = [ x > 100 for x in height ]
	birth    = [ datetime.now().isoformat()   for x in range(len(persons)) ]

	df = pd.DataFrame(persons, columns=['name', 'lastname'])
	df["EntityClass"] = entity
	df = df.reindex(["EntityClass", "name", "lastname"], axis=1)
	df["id"]       = ids
	df["age"]      = age
	df["height"]   = height
	df["has_dog"]  = dog
	df["date:dob"] = birth
	df["constraint_id"] = ids

	df = df.sort_values("id")

	df.to_csv("input/persons.adb.csv", index=False)
	print(df)

def main(params):

	generate_person_csv()


def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-numthreads', type=int, default=32)
    obj.add_argument('-batchsize',  type=int, default=100)

    # Input CSV
    obj.add_argument('-in_csv_file', type=str,
                     default="../data/hotspots/hotspots_bboxes.adb.csv")

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
