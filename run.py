from yaml import CLoader as Loader
from pprint import pprint
import pandas as pd
import datetime
import hashlib
import socket
import logging
import codecs
import luigi
import json
import yaml
import os
import re

import helper.networking
import helper.learning
import helper.pickles
import helper.ignore
import helper.files
import helper.s3
import constants

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IdentifyOoniProbeReports(luigi.ExternalTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    index_file = "ooniprobe-report-names.s3.pickle"

    def output(self):
        connection = helper.s3.connect()
        if not os.path.exists(self.index_file):
            logging.info("Creating index file %s" % self.index_file)
            dates = pd.date_range(self.start_date, self.end_date).strftime('%Y-%m-%d')
            prefixes = list(map(lambda date: os.path.join(constants.ooni_s3_targets['raw'], date), dates))
            keys = helper.s3.get_keys(
                    connection=connection,
                    prefixes=prefixes,
                    has_any=['bridge', 'tcp', 'traceroute', 'meek']
            )
            helper.pickles.save(data=keys, path=self.index_file)
        return helper.s3.wrap_as_s3_target(connection=connection, keys=helper.pickles.load(path=self.index_file))


class FetchOoniProbeReports(luigi.Task):
    """
    At this stage of the pipeline, ooni-probe reports are represented as YAML files
    """
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return IdentifyOoniProbeReports(self.start_date, self.end_date)

    def run(self):
        ignore = helper.ignore.load(path=constants.ignore_file)

        targets = set(filter(lambda t: t.path not in ignore, self.input()))
        for target in targets:
            logging.info(target.path)
            path = self.__to_target_name(prefix=constants.local_targets['raw'], target=target)
            if not os.path.exists(path):
                header, tests = self.__split_yml(target)
                if not tests:
                    ignore.add(target.path)
                else:
                    metrics = []
                    for test in tests:
                        metric = header
                        metric['report_filename'] = target.path
                        metric['test_keys'] = test
                        metrics.append(metric)
                    else:
                        helper.pickles.save(data=metrics,
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


class NormaliseOoniProbeReports(luigi.Task):
    """
    At this stage of the pipeline, ooni-probe reports are represented as pickled Python dicts - the goal is to normalize
    which keys are available within each of the test results
    """
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return FetchOoniProbeReports(self.start_date, self.end_date)

    def run(self):
        for filename in self.input():
            targets = helper.pickles.load(filename)
            metrics = []
            for target in targets:
                target = helper.learning.autocorrect(dictionary=target, required=constants.schema, relocate_to='test_keys')
                misplaced = set(target.keys()) - constants.schema
                if misplaced:
                    pprint(target)
                    raise ValueError("There are %d misplaced keys - specifically: %s" % (len(misplaced), misplaced))
                else:
                    metrics.append(target)
            else:
                helper.pickles.save(data=metrics, path=self.__to_target_name(filename))

    def output(self):
        return set(map(lambda t: luigi.file.LocalTarget(self.__to_target_name(path=t)), self.input()))

    def complete(self):
        outputs = set(filter(lambda x: not os.path.exists(x.path), self.output()))
        return len(outputs) == 0

    @staticmethod
    def __to_target_name(path):
        return path.replace(constants.local_targets['raw'], constants.local_targets['corrected'])


class SanitiseOoniProbeReports(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return NormaliseOoniProbeReports(self.start_date, self.end_date)

    def run(self):
        bridgedb = json.loads(helper.s3.get_as_string(path=constants.ooni_s3_targets['bridgedb']))

        for filename in self.input():
            targets = helper.pickles.load(filename.path)
            metrics = []
            for target in targets:
                if 'bridge' in target['test_name']:
                    self.__sanitize_bridge_reachability(target=target, bridgedb=bridgedb)
                break
            break

    def output(self):
        return set(map(lambda t: luigi.file.LocalTarget(self.__to_target_name(path=t.path)), self.input()))

    def complete(self):
        outputs = set(filter(lambda x: not os.path.exists(x.path), self.output()))
        return len(outputs) == 0

    @staticmethod
    def __to_target_name(path):
        return path.replace(constants.local_targets['corrected'], constants.local_targets['sanitised'])

    @staticmethod
    def __sanitize_bridge_reachability(target, bridgedb):
        tor_log = target['test_keys'].get('tor_log', None)
        address = target['test_keys'].pop('bridge_address', None)

        if address and address in bridgedb:
            fingerprint = codecs.decode(bridgedb[address]['fingerprint'], 'hex')
            target['test_keys']['distributor'] = bridgedb[address]['distributor']
            target['test_keys']['bridge_hashed_fingerprint'] = hashlib.sha1(fingerprint).hexdigest()
            target['input'] = target['test_keys']['bridge_hashed_fingerprint']

        if tor_log:
            for k, regexp in constants.regular_expressions['tor_log'].items():
                matches = regexp.findall(target['test_keys']['tor_log'])
                if matches:
                    if k == 'ipv4_address':
                        ips = set(filter(lambda x: helper.networking.is_ip(x), map(lambda x: ''.join(x), matches)))
                        for ip in ips:
                            logging.debug("Redacting IPv4 address %s from Tor log" % ip)
                            target['test_keys']['tor_log'] = re.sub(re.compile(ip), "[REDACTED]", target['test_keys']['tor_log'])
                    else:
                        logging.info("Redacting %s from Tor log" % k)
                        target['test_keys']['tor_log'] = re.sub(regexp, "[REDACTED]", target['test_keys']['tor_log'])
        return target


def setup():
    for path in constants.local_targets.values():
        if not os.path.exists(path):
            os.mkdir(path)


def cleanup():
    if os.path.exists(IdentifyOoniProbeReports.index_file):
        logging.info("Removing S3 key name cache: %s" % IdentifyOoniProbeReports.index_file)
        os.remove(IdentifyOoniProbeReports.index_file)

if __name__ == '__main__':
    try:
        setup()
        luigi.run()
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()