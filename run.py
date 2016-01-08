from pprint import pprint
import logging
import luigi

import helper.s3
import constants

import pandas as pd
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FetchOoniProbeReports(luigi.ExternalTask):
    def output(self):
        connection = helper.s3.connect()
        dates = pd.date_range('2016-01-01', '2016-01-05').strftime('%Y-%m-%d')
        prefixes = list(map(lambda date: os.path.join(constants.ooni_s3_targets['raw']['prefix'], date), dates))
        keys = helper.s3.get_keys(
                connection=connection,
                bucket_name=constants.ooni_s3_targets['raw']['bucket'],
                prefixes=prefixes,
                has_any=['traceroute']
        )
        return helper.s3.wrap_as_s3_target(
                connection=connection,
                bucket_name=constants.ooni_s3_targets['raw']['bucket'],
                keys=keys)

if __name__ == '__main__':
    luigi.run()