from pprint import pprint
import luigi.s3
import constants
import logging


def connect():
    return luigi.s3.S3Client(
            aws_access_key_id=constants.credentials['aws_access_key_id'],
            aws_secret_access_key=constants.credentials['aws_secret_access_key'],
    )


def get_keys(connection, prefix, has_any=None, has_all=None, date_prefix=None):
    logging.info("Getting keys from S3 with prefix: %s" % prefix)
    keys = set(connection.list(prefix))
    if date_prefix:
        keys = set(filter(lambda k: k.startswith(date_prefix), keys))
    if has_any:
        keys = set(filter(lambda k: any(map(lambda t: t in k, has_any)), keys))
    if has_all:
        keys = set(filter(lambda k: all(map(lambda t: t in k, has_all)), keys))
    return keys