import argparse
import random
import math

from datetime import datetime

import pandas as pd

from itertools import product

def generate_person_csv(multiplier):

    names     = ["James", "Luis", "Sole", "Maria", "Tom", "Xavi", "Dimitris", "King"]
    lastnames = ["Ramirez", "Berlusconi", "Copola", "Tomson", "Ferro"]
    names     = names     * multiplier
    lastnames = lastnames * multiplier

    persons  = list(product(names, lastnames))

    entity   = [ "Person"                       for x in range(len(persons))]
    ids      = [int(1000000000* random.random()) for i in range(len(persons))]
    age      = [int(100* random.random())       for i in range(len(persons))]
    height   = [float(200* random.random())     for i in range(len(persons))]
    dog      = [ x > 100 for x in height ]
    birth    = [ datetime.now().isoformat()     for x in range(len(persons)) ]

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

    return df

def generate_images_csv(multiplier):

    multiplier = multiplier // 2
    path    = "input/images/"
    imgs    = [path + str(x).zfill(4) + ".jpg" for x in range(200)] * multiplier
    license = [x for x in range(10)] * multiplier

    images  = list(product(imgs, license))

    ids      = [int(1000000000* random.random()) for i in range(len(images))]
    age      = [int(100* random.random())       for i in range(len(images))]
    height   = [float(200* random.random())     for i in range(len(images))]
    dog      = [ x > 100 for x in height ]
    date_cap = [ datetime.now().isoformat()     for x in range(len(images)) ]

    df = pd.DataFrame(images, columns=['filename', 'license'])
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids

    df = df.sort_values("id")

    df.to_csv("input/images.adb.csv", index=False)

    return df

def generate_connections_csv(persons, images):

    connections  = list(product(images["id"][::100], persons["id"][::100]))

    connect   = [ "has_image"   for x in range(len(connections))]
    confidence  = [ random.random()   for x in range(len(connections))]

    df = pd.DataFrame(connections, columns=['VD:IMG@id', 'Person@id'])
    df["ConnectionClass"] = connect
    df = df.reindex(["ConnectionClass", 'VD:IMG@id', 'Person@id'], axis=1)

    df["confidence"]      = confidence

    df.to_csv("input/connections-persons-images.adb.csv", index=False)

def generate_bboxes_csv(images):

    images  = images["id"]

    x   = [ 0   for x in range(len(images))]
    y   = [ 0   for x in range(len(images))]
    w   = [ 100 for x in range(len(images))]
    h   = [ 150 for x in range(len(images))]

    labels = ["dog", "cat", "catdog", "rocko", "philip", "froghead", "drog"]

    confidence  = [ random.random()   for x in range(len(images))]
    label       = [ labels[math.floor(random.random()*len(labels))]  for x in range(len(images))]

    df = pd.DataFrame(images, columns=['id'])

    df["x_pos"] = x
    df["y_pos"] = y
    df["width"] = w
    df["height"] = h
    df["confidence"] = confidence
    df["label"]      = label

    df = df.sort_values("id")
    df.to_csv("input/bboxes.adb.csv", index=False)

def main(params):

    persons = generate_person_csv(params.multiplier)
    images  = generate_images_csv(int(params.multiplier/2))
    connect = generate_connections_csv(persons, images)
    bboxes  = generate_bboxes_csv(images)

def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-multiplier', type=int, default=10)

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    args = get_args()
    main(args)
