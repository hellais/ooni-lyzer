from pprint import pprint
import itertools
import functools
import luigi.s3
import luigi
import logging
import boto

import helper.s3
import constants

import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FetchOoniProbeReports(luigi.ExternalTask):
    """
    Fetches raw, unsanitised ooni-probe reports from AWS S3
    """
    def output(self):
        return self.get_raw_report_keys_from_s3()


class OoniProbeReportsToJson(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass


def get_raw_report_keys_from_s3():
    connection = luigi.s3.S3Client(
            aws_access_key_id=constants.credentials['aws_access_key_id'],
            aws_secret_access_key=constants.credentials['aws_secret_access_key'],
    )
    dates = tuple(pd.date_range('2016-01-01', '2016-01-05').strftime('%Y-%m-%d'))
    targets = set()
    keys = helper.s3.get_keys(connection=connection, prefix=constants.ooni_s3_targets['raw'], date_prefix=dates)
    return keys

if __name__ == '__main__':
    get_raw_report_keys_from_s3()

    #luigi.run()