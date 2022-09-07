import argparse
import random
import math

from datetime import datetime

import pandas as pd
import numpy as np

from itertools import product


def generate_person_csv(multiplier):

    names     = ["James", "Luis", "Sole", "Maria",
                 "Tom", "Xavi", "Dimitris", "King"]
    lastnames = ["Ramirez", "Berlusconi", "Copola", "Tomson", "Ferro"]
    names     = names     * multiplier
    lastnames = lastnames * multiplier

    persons  = list(product(names, lastnames))

    entity   = ["Person" for x in range(len(persons))]
    ids      = random.sample(range(1000000000), len(persons))
    age      = [int(100 * random.random()) for i in range(len(persons))]
    height   = [float(200 * random.random()) for i in range(len(persons))]
    dog      = [x > 100 for x in height]
    birth    = [datetime.now().isoformat() for x in range(len(persons))]

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


def generate_blobs_csv():

    path       = "input/blobs/"
    blob_paths = [path + str(x).zfill(4) + ".blob" for x in range(20)]
    license    = [x for x in range(10)]
    blobs      = list(product(blob_paths, license))

    # generate the blobs
    arr = [1.5, 2.4, 3.3, 5.5, 9.9, 111.12, 1000.20]
    nparr = np.array(arr)
    for blob in blob_paths:
        blobfile = open(blob, 'wb')
        nparr.tofile(blobfile)
        blobfile.close()

    entity   = ["segmentation" for x in range(len(blobs))]
    ids      = random.sample(range(1000000000), len(blobs))

    df = pd.DataFrame(blobs, columns=['filename', 'license'])
    df                  = df.reindex(["filename", "license"], axis=1)
    df["id"]            = ids
    df["constraint_id"] = ids

    df = df.sort_values("id")

    df.to_csv("input/blobs.adb.csv", index=False)

    return df


def generate_images_csv(multiplier):

    multiplier = multiplier // 2
    path    = "input/images/"
    imgs    = [path + str(x).zfill(4) +
               ".jpg" for x in range(200)] * multiplier
    license = [x for x in range(10)] * multiplier

    images  = list(product(imgs, license))

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]

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


def generate_http_images_csv(ip_file_csv):

    images    = pd.read_csv(ip_file_csv, sep=",", header=None)

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    license  = [x for x in range(len(images))]

    df = pd.DataFrame()
    df['url']      = images
    df["urlid"]    = ids
    df['license']  = license
    df["age"]      = age
    df["height"]   = height
    df["constraint_urlid"] = ids

    df = df.sort_values("urlid")

    df.to_csv("input/http_images.adb.csv", index=False)

    return df


def generate_s3_images_csv(ip_file_csv):

    images    = pd.read_csv(ip_file_csv, sep=",", header=None)

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    license  = [x for x in range(len(images))]

    df = pd.DataFrame()
    df['s3_url']   = images
    df["id"]       = ids
    df['license']  = license
    df["age"]      = age
    df["height"]   = height
    df["constraint_id"] = ids

    df = df.sort_values("id")

    df.to_csv("input/s3_images.adb.csv", index=False)

    return df


def generate_gs_images_csv(ip_file_csv):

    images    = pd.read_csv(ip_file_csv, sep=",", header=None)

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    license  = [x for x in range(len(images))]

    df = pd.DataFrame()
    df['gs_url']   = images
    df["id"]       = ids
    df['license']  = license
    df["age"]      = age
    df["height"]   = height
    df["constraint_id"] = ids

    df = df.sort_values("id")

    df.to_csv("input/gs_images.adb.csv", index=False)

    return df


def generate_connections_csv(persons, images):

    connections  = list(product(images["id"][::100], persons["id"][::100]))

    connect   = ["has_image" for x in range(len(connections))]
    confidence  = [random.random() for x in range(len(connections))]

    df = pd.DataFrame(connections, columns=['_Image@id', 'Person@id'])
    df["ConnectionClass"] = connect
    df = df.reindex(["ConnectionClass", '_Image@id', 'Person@id'], axis=1)

    df["confidence"]      = confidence

    df.to_csv("input/connections-persons-images.adb.csv", index=False)


def generate_bboxes_csv(images):

    images  = images["id"]

    x   = [0 for x in range(len(images))]
    y   = [0 for x in range(len(images))]
    w   = [100 for x in range(len(images))]
    h   = [150 for x in range(len(images))]

    labels = ["dog", "cat", "catdog", "rocko", "philip", "froghead", "drog"]

    confidence  = [random.random() for x in range(len(images))]
    label       = [labels[math.floor(random.random() * len(labels))]
                   for x in range(len(images))]

    df = pd.DataFrame(images, columns=['id'])

    df["x_pos"] = x
    df["y_pos"] = y
    df["width"] = w
    df["height"] = h
    df["confidence"] = confidence
    df["label"]      = label

    df = df.sort_values("id")
    df.to_csv("input/bboxes.adb.csv", index=False)


def generate_bboxes_constraints_csv(images):
    image_id = images.iloc[0]["id"]
    num_boxes = 3

    x   = [0 for x in range(num_boxes)]
    y   = [0 for x in range(num_boxes)]
    w   = [100 for x in range(num_boxes)]
    h   = [150 for x in range(num_boxes)]

    labels = ["dog", "cat", "catdog", "rocko", "philip", "froghead", "drog"]

    confidence  = [random.random() for x in range(num_boxes)]
    label       = [labels[math.floor(random.random() * num_boxes)]
                   for x in range(num_boxes)]

    df = pd.DataFrame([{
        "id": image_id,
    } for _ in range(num_boxes)])

    df["x_pos"] = x
    df["y_pos"] = y
    df["width"] = w
    df["height"] = h
    df["confidence"] = confidence
    df["label"]      = label
    df["box_id"] = [123] * num_boxes
    df["constraint_box_id"] = [123] * num_boxes

    df = df.sort_values("id")
    df.to_csv("input/bboxes-constraints.adb.csv", index=False)


def generate_descriptors(images, setname, dims):

    filename = "input/" + setname + "_desc.npy"

    images  = images["id"]

    descriptors = np.random.rand(len(images), dims)

    np.save(filename, descriptors)

    labels = ["dog", "cat", "catdog", "rocko", "philip", "froghead", "drog"]
    confidence  = [random.random() for x in range(len(images))]
    label       = [labels[math.floor(random.random() * len(labels))]
                   for x in range(len(images))]

    df = pd.DataFrame(images, columns=['id'])

    df["filename"] = [filename for x in range(len(images))]
    df["index"]    = [i for i in range(len(images))]
    df["set"]      = [setname for x in range(len(images))]
    df["id"]       = [i for i in range(len(images))]
    df["label"]      = label
    df["confidence"] = confidence

    df = df.sort_values("id")
    df = df.reindex(columns=["filename", "index",
                    "set", "id", "label", "confidence"])
    df.to_csv("input/" + setname + ".adb.csv", index=False)


def generate_descriptorset(names, dims):

    metrics = ["L2", "IP", "CS", ["CS", "IP"], ["L2", "CS", "IP"]]
    engines = ["FaissIVFFlat", "FaissFlat", ["FaissIVFFlat", "FaissFlat"]]

    df = pd.DataFrame()

    df["name"]       = names
    df["dimensions"] = dims
    df["engine"]     = [random.sample(engines, 1)[0] for i in names]
    df["metric"]     = [random.sample(metrics, 1)[0] for i in names]

    df.to_csv("input/descriptorset.adb.csv", index=False)


def main(params):

    persons = generate_person_csv(params.multiplier)
    blobs   = generate_blobs_csv()
    images  = generate_images_csv(int(params.multiplier / 2))
    s3_imgs = generate_http_images_csv("input/sample_http_urls.csv")
    s3_imgs = generate_s3_images_csv("input/sample_s3_urls.csv")
    generate_gs_images_csv("input/sample_gs_urls")
    connect = generate_connections_csv(persons, images)
    bboxes  = generate_bboxes_csv(images)
    bboxes_constraints = generate_bboxes_constraints_csv(images)

    desc_name = ["setA", "setB", "setC", "setD", "setE", "setF"]
    desc_dims = [2048, 1025, 2048, 1025, 2048, 1025]    # yes, 1025
    generate_descriptorset(desc_name, desc_dims)
    for name, dims in zip(desc_name, desc_dims):
        generate_descriptors(images, name, dims)


def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-multiplier', type=int, default=10)

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
