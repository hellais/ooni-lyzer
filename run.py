from yaml import CLoader as Loader
from pprint import pprint
import datetime
import logging
import luigi
import yaml

import helper.pickles
import helper.ignore
import helper.files
import helper.s3
import constants

import pandas as pd
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IdentifyOoniProbeReports(luigi.ExternalTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        connection = helper.s3.connect()
        dates = pd.date_range(self.start_date, self.end_date).strftime('%Y-%m-%d')
        prefixes = list(map(lambda date: os.path.join(constants.ooni_s3_targets['raw'], date), dates))
        keys = helper.s3.get_keys(
                connection=connection,
                prefixes=prefixes,
                has_any=['http_invalid_request_line']
        )
        return helper.s3.wrap_as_s3_target(
                connection=connection,
                keys=keys)


class FetchOoniProbeReports(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return IdentifyOoniProbeReports(self.start_date, self.end_date)

    def run(self):
        ignore = helper.ignore.load(path=constants.ignore_file)

        targets = set(filter(lambda t: t.path not in ignore, self.input()))
        for target in targets:
            path = self.__to_target_name(prefix=constants.local_targets['raw'], target=target)
            if not os.path.exists(path):
                header, tests = self.__split_yml(target)
                if not tests:
                    ignore.add(target.path)
                else:
                    for test in tests:
                        metric = header
                        metric['test_keys'] = test
                        helper.pickles.save(data=metric,
                                            path=self.__to_target_name(
                                                 prefix=constants.local_targets['raw'],
                                                 target=target))
        else:
            helper.ignore.update(path=constants.ignore_file, data=ignore)

    def output(self):
        ignore = helper.ignore.load(path=constants.ignore_file)
        targets = filter(lambda t: t.path not in ignore, self.input())
        return list(map(lambda t: self.__to_target_name(prefix=constants.local_targets['raw'], target=t), targets))

    def complete(self):
        ignore = helper.ignore.load(path=constants.ignore_file)
        inputs = set(filter(lambda x: x.path not in ignore, self.input()))
        outputs = set(filter(lambda x: not os.path.exists(self.__to_target_name(
                prefix=constants.local_targets['raw'],
                target=x)), inputs))
        return len(outputs) == 0

    @staticmethod
    def __split_yml(fh):
        yml = list(yaml.load_all(fh.open(), Loader=Loader))
        return yml[0], yml[1:]

    @staticmethod
    def __to_target_name(prefix, target):
        target = os.path.join(prefix, target.path.split('/')[-1:][0])
        return helper.files.set_extension(path=target, ext='pickle')


class NormalizeOoniProbeReports(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return FetchOoniProbeReports(self.start_date, self.end_date)

    def run(self):
        for target in self.input():
            print(target)

    def output(self):
        pass


if __name__ == '__main__':
    luigi.run()