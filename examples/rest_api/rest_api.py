import requests
import argparse
import json
import os
from aperturedb.CommonLibrary import create_connector
from aperturedb.Connector import Connector

client: Connector = create_connector()

URL = "https://" + client.config.host  + '/api'

VERIFY_SSL = True


def parse_auth(res):

    res = json.loads(res)["json"]
    print(json.dumps(res, indent=4, sort_keys=False))

    session_token = res[0]["Authenticate"]["session_token"]
    refresh_token = res[0]["Authenticate"]["refresh_token"]
    return session_token, refresh_token


def auth():

    query = [{
        "Authenticate": {
            "username": client.config.username,
            "password": client.config.password,
        }
    }]

    # Authenticate
    response = requests.post(URL,
                             files = [('query', (None, json.dumps(query)))],
                             verify = VERIFY_SSL)

    # print(response.status_code)
    # print(response.text)

    return parse_auth(response.text)


def query_api(query, st, files_upload=[]):

    files = [
        ('query', (None, json.dumps(query))),
    ]

    for file in files_upload:
        instream = open(file, 'rb')
        files.append(
            ('blobs', (os.path.basename(file), instream, 'image/jpeg')))

    response = requests.post(URL,
                             headers = {'Authorization': "Bearer " + st},
                             files   = files,
                             verify  = VERIFY_SSL)

    # Parse response:
    try:
        json_response = json.loads(response.text)
        response      = json_response["json"]
        blobs         = json_response["blobs"]
    except:
        print("Error with response:")
        print(response.status_code)
        print(response.text)
        response = "error!"
        blobs = []

    return response, blobs


def get_status(st):

    query = [{
        "GetStatus": {}
    }]

    return query_api(query, st)


def add_image_by_id(st, id):

    query = [{
        "AddImage": {
            "properties": {
                "rest_api_example_id": id
            }
        }
    }]

    return query_api(query, st, files_upload=["songbird.jpg"])


def get_image_by_id(st, id):

    query = [{
        "FindImage": {
            "constraints": {
                "_uniqueid": ["==", id]
            },
            "results": {
                "all_properties": True
            }
        }
    }]

    return query_api(query, st)


def list_images(st):

    query = [{
        "FindImage": {
            "blobs": False,
            "uniqueids": True
        }
    }]

    return query_api(query, st)


def main(params):

    VERIFY_SSL = params.verify_ssl

    print("-" * 80)
    print("Authentication:")
    session_token, refresh_token = auth()

    # Print DB Status
    # get_status(session_token)

    # ----------------------
    print("-" * 80)
    print("List Images:")
    r, blobs = list_images(session_token)
    print("Response:")
    print(json.dumps(r, indent=4, sort_keys=False))
    img_id = r[0]["FindImage"]["entities"][0]["_uniqueid"]

    # ----------------------
    print("-" * 80)
    print("Find image by id:")
    r, blobs = get_image_by_id(session_token, img_id)

    print("Response:")
    print(json.dumps(r, indent=4, sort_keys=False))

    print("Returned images: {}".format(len(blobs)))

    # Base 64 encoded images
    for img in blobs:

        print("Image size (base64 enconded): {}".format(len(img)))

    # ----------------------
    print("-" * 80)
    print("Add image by id:")
    r, blobs = add_image_by_id(session_token, 123456789)

    print("Response:")
    print(json.dumps(r, indent=4, sort_keys=False))

    # ----------------------


def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('-verify_ssl',  type=bool, default=True)

    params = obj.parse_args()

    return params


if __name__ == "__main__":
    args = get_args()
    main(args)
