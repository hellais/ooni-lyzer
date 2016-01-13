from pprint import pprint
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


def get(s3_path, local_path, connection=connect(), decode='utf-8'):
    return connection.get(s3_path=s3_path, destination_local_path=local_path)


def get_keys(connection, prefixes, has_any=None, has_all=None):
    keys = set()
    if len(prefixes) > 64:
        logging.info("Since there are %d prefixes, I will fetch all S3 keys, and filter accordingly" % (len(prefixes)))
        prefix = os.path.dirname(prefixes[0])
        keys = set(filter(lambda k: any(map(lambda t: t in k, prefixes)), map(lambda k: os.path.join(prefix, k), connection.list(prefix))))
    else:
        for p in prefixes:
            logging.info("Collecting keys for prefix: %s" % p)
            keys.update(set(map(lambda k: os.path.join(p, k), connection.list(p))))
    if has_any:
        keys = set(filter(lambda k: any(map(lambda t: t in k, has_any)), keys))
    if has_all:
        keys = set(filter(lambda k: all(map(lambda t: t in k, has_all)), keys))
    return keys


def wrap_as_s3_target(connection, keys):
    targets = []
    for key in keys:
        targets.append(luigi.s3.S3Target(key, client=connection))
    return targets