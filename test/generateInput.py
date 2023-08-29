import argparse
import random
import math
import os

from datetime import datetime

import pandas as pd
import numpy as np
from generateImages import ImageGenerator

from itertools import product


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    else:
        return text


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

    path       = "blobs/"
    blob_paths = [path + str(x).zfill(4) + ".blob" for x in range(20)]
    license    = [x for x in range(10)]
    blobs      = list(product(blob_paths, license))

    # generate the blobs
    arr = [1.5, 2.4, 3.3, 5.5, 9.9, 111.12, 1000.20]
    nparr = np.array(arr)
    for blob in blob_paths:
        with open(os.path.join("input", blob), 'wb') as blobfile:
            nparr.tofile(blobfile)

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
    path    = "images/"
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

    filename = setname + "_desc.npy"

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


def generate_update_person():
    # generate 3 testing csvs for update testing
    # cvs 1 - base load with version.
    df = pd.read_csv("input/persons.adb.csv")

    # set version column to 1.
    version_id = [1 for x in range(len(df))]
    df["updateif_<version_id"] = version_id
    df["version_id"] = version_id
    df.to_csv("input/persons-update.adb.csv", index=False)

    # csv 2 - modified data, but same version ( will cause no updates )
    df['age'] = df['age'].apply(lambda age: age + 200)
    df['version_id'] = df['version_id'].apply(lambda ver: ver + 1)
    df.to_csv("input/persons-update-oldversion.adb.csv", index=False)

    # csv 3 - modified data, and version ( will update )
    # update in database if version < 2.
    df['updateif_<version_id'] = df['version_id']
    df.to_csv("input/persons-update-newversion.adb.csv", index=False)

    # generate updateif to test > comparator and partial update
    df = pd.read_csv("input/persons.adb.csv")
    df['age'] = df['age'].apply(lambda age: age + 200)
    # change if age in database is > 30
    df['updateif_>age'] = [30 for x in range(len(df))]
    df = df.to_csv("input/persons-update-olderage.adb.csv", index=False)


def generate_partial_load():

    df = pd.read_csv("input/persons.adb.csv")
    base = df.head(10)
    base.to_csv("input/persons-exist-base.adb.csv", index=False)
    # causes 3 overlaps
    overlapped = df.head(12).tail(5)
    overlapped.to_csv("input/persons-some-exist.adb.csv", index=False)


def generate_update_images(multiplier):

    image_count = 5
    licence_count = 2
    multiplier = multiplier // 2
    path    = "images/"
    imgs    = [path + "number_" + str(i).zfill(4) +
               ".png" for i in range(image_count)] * multiplier
    license = [x for x in range(licence_count)] * multiplier

    images  = list(product(imgs, license))
    duplicate_count = len(images) // 10

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]

    df = pd.DataFrame(images, columns=['filename', 'license'])
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids
    df["version_id"] = version_id
    df["updateif_<version_id"] = version_id

    df = df.sort_values("id")

    updates = df.head(duplicate_count).copy()
    updated_version_id = [2 for i in range(duplicate_count)]
    updates["version_id"] = updated_version_id
    updates["updateif_<version_id"] = updated_version_id
    df = pd.concat([df, updates])

    df.to_csv("./input/images_update_and_add.adb.csv", index=False)


def generate_newest_images(multiplier):
    image_count = 5
    licence_count = 2
    multiplier = multiplier // 2
    path    = "images/"
    img_ids = [i for i in range(image_count)] * multiplier
    def filegen(file_num): return path + "number_" + \
        str(file_num).zfill(4) + ".png"
    license = [x for x in range(licence_count)] * multiplier

    images  = list(product(img_ids, license))
    prop_change_count = len(images) // 10

    ids      = random.sample(range(1000000000), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]
    empty_column = ["" for x in range(len(images))]

    df = pd.DataFrame(images, columns=['img_id', 'license'])
    # we want filename to be first column
    df.insert(0, "filename", df['img_id'].apply(filegen))
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids
    df["version_id"] = version_id
    df["gen_blobsha1_imagesha"] = empty_column
    df["updateif_<version_id"] = version_id

    df = df.sort_values("id")

    # temporary
    df = df.head(4).copy()
    prop_change_count = 1
    blob_change_count = 1

    prop_updates = df.head(prop_change_count).copy()
    prop_updates["version_id"] = prop_updates["version_id"].apply(
        lambda id: id + 1)
    prop_updates["updateif_<version_id"] = prop_updates["version_id"]
    prop_updates["age"] = prop_updates["age"].apply(lambda age: age + 1)

    blob_updates = df.head(prop_change_count + 1).tail(1).copy()
    blob_updates["version_id"] = blob_updates["version_id"].apply(
        lambda id: id + 2)
    blob_updates["updateif_<version_id"] = blob_updates["version_id"]
    blob_updates["age"] = blob_updates["age"].apply(lambda age: age + 2)
    blob_updates["img_id"] = blob_updates["img_id"].apply(lambda id: id + 1)
    blob_updates["filename"] = blob_updates["img_id"].apply(filegen)
    df = pd.concat([df, prop_updates, blob_updates])
    print(df)
    df = df.drop(columns=["img_id"])

    df.to_csv("./input/images_newest_blobs.adb.csv", index=False)


def generate_update_image(multiplier):
    # generate base load
    # generate images
    image_count = 100
    img_gen = ImageGenerator(count=image_count, output="input/images/update_images_%%.png", image_type="png",
                             size=(256, 256), manifest="input/update_image_list.csv")
    img_gen.run()
    img_df = pd.read_csv("input/update_image_list.csv", header=None)
    licence_count = 2
    multiplier = multiplier // 2
    img_ids = [i for i in range(image_count)] * multiplier
    license = [x for x in range(licence_count)] * multiplier

    def filemap(file_num):
        return remove_prefix(img_df.iat[file_num % image_count, 0], "input/")
    images  = list(product(img_ids, license))
    prop_change_count = len(images) // 10
    id_range = 500000000
    # id_range=1000000000
    df = pd.DataFrame(images, columns=['img_id', 'license'])
    ids      = random.sample(range(id_range), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]

    df = pd.DataFrame(images, columns=['img_id', 'license'])
    # we want filename to be first column
    df.insert(0, "filename", df['img_id'].apply(filemap))
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids
    df["version_id"] = version_id
    df["updateif_<version_id"] = version_id

    df.to_csv("./input/images_updateif_baseload.adb.csv", index=False)

    # csv 2: original content plus new to verify blob loading with only partial loads, randomize
    id_range_start = id_range
    id_range_end = 1000000000
    new_start = image_count
    new_end = int(image_count * 1.1)
    img_ids = [i for i in range(new_start, new_end)] * multiplier
    images  = list(product(img_ids, license))
    prop_change_count = len(images) // 10
    additional_df = pd.DataFrame(images, columns=['img_id', 'license'])
    ids      = random.sample(range(id_range_start, id_range_end), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]

    # we want filename to be first column
    additional_df.insert(0, "filename", df['img_id'].apply(filemap))
    additional_df["id"]       = ids
    additional_df["age"]      = age
    additional_df["height"]   = height
    additional_df["has_dog"]  = dog
    additional_df["date:date_captured"] = date_cap
    additional_df["constraint_id"] = ids
    additional_df["version_id"] = version_id
    additional_df["updateif_<version_id"] = version_id

    additional_df.to_csv(
        "./input/images_updateif_mixednew1.adb.csv", index=False)
    # mix the new records in with the old.
    combined_df = pd.concat([df, additional_df],
                            ignore_index=True).sample(frac=1)

    combined_df.to_csv("./input/images_updateif_mixednew.adb.csv", index=False)


def generate_update_image_fail(multiplier):
    # generate base load, small images.
    image_count = 100
    img_gen = ImageGenerator(count=image_count, output="input/images/update_fail_images_%%.png", image_type="png",
                             size=(32, 32), manifest="input/update_fail_image_list.csv")
    img_gen.run()
    img_df = pd.read_csv("input/update_fail_image_list.csv", header=None)
    licence_count = 2
    multiplier = multiplier // 2
    img_ids = [i for i in range(image_count)] * multiplier
    license = [x for x in range(licence_count)] * multiplier

    def filemap(file_num):
        return remove_prefix(img_df.iat[file_num % image_count, 0], "input/")
    images  = list(product(img_ids, license))
    prop_change_count = len(images) // 10
    id_range = 500000000
    df = pd.DataFrame(images, columns=['img_id', 'license'])
    ids      = random.sample(range(id_range), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]

    df.insert(0, "filename", df['img_id'].apply(filemap))
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids
    df["version_id"] = version_id
    df["updateif_<version_id"] = version_id
    df.to_csv("./input/images_updateif_fail_baseload.adb.csv", index=False)

    # 2nd csv - original data, but new bigger images. update_id will increase, but image will remain the same. ( as blobs are not modifable )
    # 300x300 is chosen to allow twice as large - we also add some more text to ensure the image is more complex ( and thus larger )
    img_gen = ImageGenerator(count=image_count, output="input/images/update_fail_big_images_%%.png", image_type="png",
                             size=(300, 300), manifest="input/update_fail_big_image_list.csv", append_text="_bigimage")
    img_gen.run()
    big_img_df = pd.read_csv(
        "input/update_fail_big_image_list.csv", header=None)
    failing_update = df.copy()

    def big_fail_filemap(file_num):
        return remove_prefix(big_img_df.iat[file_num % image_count, 0], "input/")
    failing_update["filename"] = failing_update["img_id"].apply(
        big_fail_filemap)
    failing_update['version_id'] = failing_update['version_id'].apply(
        lambda ver: ver + 1)
    failing_update["updateif_<version_id"] = failing_update["version_id"]
    failing_update.to_csv(
        "./input/images_updateif_fail_updates.adb.csv", index=False)


def generate_forceimage_load(multiplier):
    # copy from imageupdate.
    # ensure normal load.
    base = pd.read_csv("./input/images_updateif_baseload.adb.csv")
    base.to_csv("./input/images_forceupdate_baseload.adb.csv", index=False)
    # ensure loading with partial new works as expected.
    mixednew = pd.read_csv("./input/images_updateif_mixednew.adb.csv")
    mixednew.to_csv("./input/images_forceupdate_mixednew.adb.csv", index=False)

    # test loading of modified image
    changeimage_base = pd.read_csv(
        "./input/images_updateif_fail_baseload.adb.csv")
    # with a modified id, the id will change, but not the images.
    changeimage_modified = pd.read_csv(
        "./input/images_updateif_fail_updates.adb.csv")

    changeimage_base.to_csv(
        "./input/images_forceupdate_fail_base.adb.csv", index=False)
    changeimage_modified.to_csv(
        "./input/images_forceupdate_fail_updates.adb.csv", index=False)

    # now change load to add column to verify image.
    empty_column = ["" for x in range(len(changeimage_base))]

    # add autogenerated column ( needed to seed the data )
    changeimage_base["prop_blobsha1_imagesha"] = empty_column
    # non should exist in base load, so will not effect anything
    changeimage_base["updateif_blobsha1_imagesha"] = empty_column

    # add autogenerated column to update.
    changeimage_modified["prop_blobsha1_imagesha"] = empty_column
    # required to detect blob change and remove and re-add.
    changeimage_modified["updateif_blobsha1_imagesha"] = empty_column

    changeimage_base.to_csv(
        "./input/images_forceupdate_blob_baseload.adb.csv", index=False)
    # with a modified id, the id will change, but not the images.
    changeimage_modified.to_csv(
        "./input/images_forceupdate_updates.adb.csv", index=False)


def generate_sparse_add(multiplier):
    # generate base load, small images.
    image_count = 100
    img_gen = ImageGenerator(count=image_count, output="input/images/spare_add_image_%%.png", image_type="png",
                             size=(32, 32), manifest="input/sparse_add_image_list.csv")
    img_gen.run()
    img_df = pd.read_csv("input/sparse_add_image_list.csv", header=None)
    licence_count = 2
    multiplier = multiplier // 2
    img_ids = [i for i in range(image_count)] * multiplier
    license = [x for x in range(licence_count)] * multiplier

    def filemap(file_num):
        return remove_prefix(img_df.iat[file_num % image_count, 0], "input/")
    images  = list(product(img_ids, license))
    prop_change_count = len(images) // 10
    id_range = 500000000
    df = pd.DataFrame(images, columns=['img_id', 'license'])
    ids      = random.sample(range(id_range), len(images))
    age      = [int(100 * random.random()) for i in range(len(images))]
    height   = [float(200 * random.random()) for i in range(len(images))]
    dog      = [x > 100 for x in height]
    date_cap = [datetime.now().isoformat() for x in range(len(images))]
    version_id = [1 for x in range(len(images))]

    df.insert(0, "filename", df['img_id'].apply(filemap))
    df["id"]       = ids
    df["age"]      = age
    df["height"]   = height
    df["has_dog"]  = dog
    df["date:date_captured"] = date_cap
    df["constraint_id"] = ids
    df["version_id"] = version_id
    df["updateif_<version_id"] = version_id
    # create load with first half
    half = len(df) / 2
    # won't work with uneven, since we are going to expect to be able to double number to check.
    assert(int(half) == half)
    df.head(int(half)).to_csv(
        "./input/images_sparseload_base.adb.csv", index=False)

    # then load with all, which half will not be sent, as they exist.
    df.to_csv("./input/images_sparseload_full.adb.csv", index=False)


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

    generate_update_person()
    generate_partial_load()
    generate_update_image(params.multiplier)
    generate_update_image_fail(params.multiplier)
    generate_sparse_add(params.multiplier)
    generate_forceimage_load(params.multiplier)


def get_args():
    obj = argparse.ArgumentParser()

    # Run Config
    obj.add_argument('-multiplier', type=int, default=10)

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
