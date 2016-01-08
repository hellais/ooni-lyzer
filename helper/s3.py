from pprint import pprint
import pandas as pd

from boto.s3.connection import S3Connection
import constants
import itertools
import luigi.s3
import logging
import boto
import os

boto.set_stream_logger('boto')
logging.getLogger('boto').setLevel(logging.INFO)


def connect():
    return S3Connection(
            aws_access_key_id=constants.credentials['aws_access_key_id'],
            aws_secret_access_key=constants.credentials['aws_secret_access_key']
    )


def wrap_as_s3_target(connection, bucket_name, keys):
    targets = []
    for key in keys:
        key = os.path.join('s3://', bucket_name, key.name)
        targets.append(luigi.s3.S3Target(key))
    return targets


def get_keys(connection, bucket_name, prefixes, has_any=None, has_all=None):
    logging.info("Getting keys from %s" % bucket_name)
    bucket = connection.get_bucket(bucket_name=bucket_name)
    keys = itertools.chain(*map(lambda e: e, map(lambda p: bucket.list(p), prefixes)))
    if has_any:
        keys = filter(lambda k: any(map(lambda t: t in k.name, has_any)), keys)
    if has_all:
        keys = filter(lambda k: all(map(lambda t: t in k.name, has_all)), keys)
    return set(keys)