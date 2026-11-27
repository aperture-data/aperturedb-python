import os
import argparse

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def main(params):

    client = WebClient(token=params.token)

    client.chat_postMessage(channel="#" + params.channel, text=params.msg)

    return 0


def get_args():
    obj = argparse.ArgumentParser()

    # Remember to always add a minimal mode to your benchmark
    obj.add_argument('-channel',  type=str,
                     default=os.environ.get('SLACK_CHANNEL', "cronjobs"))

    obj.add_argument('-token',  type=str,
                     default=os.environ.get('SLACK_BOT_TOKEN', ""))

    obj.add_argument('-msg',  type=str, default="no message specified")

    params = obj.parse_args()

    return params


def run():
    main(get_args())


if __name__ == "__main__":
    run()
