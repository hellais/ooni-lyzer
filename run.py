from pprint import pprint
import luigi.s3
import luigi
import logging

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
        connection = luigi.s3.S3Client(
                aws_access_key_id=constants.credentials['aws_access_key_id'],
                aws_secret_access_key=constants.credentials['aws_secret_access_key'],
        )
        dates = tuple(pd.date_range('2016-01-01', '2016-01-05').strftime('%Y-%m-%d'))
        keys = helper.s3.get_keys(
                connection=connection,
                prefix=constants.ooni_s3_targets['raw'],
                date_prefix=dates,
                has_any=['traceroute'])
        return helper.s3.wrap_as_s3_target(connection=connection, keys=keys)


class OoniProbeReportsToJson(luigi.Task):
    def requires(self):
        return FetchOoniProbeReports()

    def run(self):
        print("Hello")

    def output(self):
        pass


if __name__ == '__main__':
    luigi.run()