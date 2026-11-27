#!/usr/bin/env python3

import argparse

from aperturedb.CommonLibrary import create_connector


def main(args):

    db = create_connector()
    query = [dict(UserLogMessage=dict(
        type=args.level, text=" ".join(args.text)))]
    status, _ = db.query(query)
    assert 0 == status[0]['UserLogMessage']['status'], (query, status)


def get_args():
    obj = argparse.ArgumentParser()
    obj.add_argument('--level', '-l', nargs=1, type=str, default="INFO")
    obj.add_argument('--debug', '-d', dest='level',
                     action='store_const', const="DEBUG")
    obj.add_argument('--warning', '--warn', '-w', dest='level',
                     action='store_const', const="WARNING")
    obj.add_argument('--info',  '-i', dest='level',
                     action='store_const', const="INFO")
    obj.add_argument('--error', '--err', '-e', dest='level',
                     action='store_const', const="ERROR")
    obj.add_argument('--critical', '-c', dest='level',
                     action='store_const', const="CRITICAl")
    obj.add_argument('text', nargs=argparse.REMAINDER, type=str, default='')
    args = obj.parse_args()

    return args


def run():
    main(get_args())


if __name__ == "__main__":
    run()
