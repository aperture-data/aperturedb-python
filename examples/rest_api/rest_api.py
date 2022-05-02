import dbinfo
import requests
import json

URL = "http://" + dbinfo.DB_HOST + '/api'


def dict_query_to_str(dict_query):

    return str(json.dumps(dict_query)).replace("'", '\"')


def parse_auth(res):

    res = json.loads(json.loads(res)["json"])
    print(res)

    session_token = res[0]["Authenticate"]["session_token"]
    refresh_token = res[0]["Authenticate"]["refresh_token"]
    return session_token, refresh_token


def auth():

    query = [{
        "Authenticate": {
            "username": dbinfo.DB_USER,
            "password": dbinfo.DB_PASS,
        }
    }]

    query = dict_query_to_str(query)

    # Authenticate
    response = requests.post(URL, data = {'json_query': query})

    # print(response.status_code)
    # print(response.text)

    return parse_auth(response.text)


def query_api(query, st):

    query = dict_query_to_str(query)
    # print(repr(query))

    response = requests.post(URL, headers = {'Authorization': "Bearer " + st},
                             data    = {'json_query': query})

    # print(response.status_code)
    # print(response.text)

    # Parse response:
    json_response = json.loads(response.text)
    response      = json.loads(json_response["json"])
    blobs         = json_response["blobs"]

    return response, blobs


def get_status(st):

    query = [{
        "GetStatus": {}
    }]

    return query_api(query, st)


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


def main():

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

    # ----------------------
    print("-" * 80)
    print("Find image by id:")
    r, blobs = get_image_by_id(session_token, 1122609)

    print("Response:")
    print(json.dumps(r, indent=4, sort_keys=False))

    print("Returned images: {}".format(len(blobs)))

    # Base 64 encoded images
    for img in blobs:

        print("Image size (base64 enconded): {}".format(len(img)))

    # ----------------------


if __name__ == "__main__":
    main()
