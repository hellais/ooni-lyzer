import constants
import luigi.s3
import logging
import boto
import os

boto.set_stream_logger('boto')
logging.getLogger('boto').setLevel(logging.INFO)


def connect():
    return luigi.s3.S3Client(
            aws_access_key_id=constants.credentials['aws_access_key_id'],
            aws_secret_access_key=constants.credentials['aws_secret_access_key']
    )


def get_as_string(path, connection=connect(), decode='utf-8'):
    return connection.get_as_string(path).decode(decode)


def get_keys(connection, prefixes, has_any=None, has_all=None):
    keys = set()
    for p in prefixes:
        logging.info("Collecting keys for prefix: %s" % p)
        keys.update(set(map(lambda k: os.path.join(p, k), connection.list(p))))
    logging.info("Filtering %d keys" % len(keys))
    if has_any:
        keys = set(filter(lambda k: any(map(lambda t: t in k, has_any)), keys))
    if has_all:
        keys = set(filter(lambda k: all(map(lambda t: t in k, has_all)), keys))
    logging.info("Finished filtering - there are %d keys left" % len(keys))
    return keys


def wrap_as_s3_target(connection, keys):
    targets = []
    for key in keys:
        targets.append(luigi.s3.S3Target(key, client=connection))
    return targets