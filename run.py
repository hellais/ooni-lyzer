from yaml import CLoader as Loader
from twisted.names.dns import Query
from pprint import pprint
import pandas as pd
import twisted.names
import datetime
import hashlib
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
import helper.jsons
import helper.s3
import constants

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Identify(luigi.ExternalTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    index_file = "ooniprobe-report-names.s3.pickle"

    def output(self):
        connection = helper.s3.connect()
        if not os.path.exists(self.index_file):
            logging.info("Creating index file %s" % self.index_file)
            dates = pd.date_range(self.start_date, self.end_date).strftime('%Y-%m-%d')
            prefixes = list(map(lambda date: os.path.join(constants.s3_targets['raw'], date), dates))
            keys = helper.s3.get_keys(
                    connection=connection,
                    prefixes=prefixes,
                    has_any=constants.test_categories['dnst'],
            )
            helper.pickles.save(data=keys, path=self.index_file)
        return helper.s3.wrap_as_s3_target(connection=connection, keys=helper.pickles.load(path=self.index_file))

    def complete(self):
        return os.path.exists(self.index_file)


class Fetch(luigi.Task):
    """
    At this stage of the pipeline, ooni-probe reports are represented as YAML files
    """
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Identify(self.start_date, self.end_date)

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


class Normalise(luigi.Task):
    """
    At this stage of the pipeline, ooni-probe reports are represented as pickled Python dicts - the goal is to normalize
    which keys are available within each of the test results
    """
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Fetch(self.start_date, self.end_date)

    def run(self):
        for filename in self.input():
            logging.info("Normalising %s" % filename)

            targets = helper.pickles.load(filename)
            metrics = []
            for target in targets:
                target = self.normalize(path=filename, target=target)

    def output(self):
        return set(map(lambda t: luigi.file.LocalTarget(self.__to_target_name(path=t)), self.input()))

    def complete(self):
        outputs = set(filter(lambda x: not os.path.exists(x.path), self.output()))
        return len(outputs) == 0

    def normalize(self, path, target):
        target = self.__normalize_generic(target=target)
        if any(map(lambda t: t in path, constants.test_categories['dnst'])):
            target = self.__normalize_dnst(target=target, path=path)
        if any(map(lambda t: t in path, constants.test_categories['httpt'])):
            target = self.__normalize_httpt(target=target, path=path)
        if any(map(lambda t: t in path, constants.test_categories['scapyt'])):
            target = self.__normalize_scapyt(target=target, path=path)
        if any(map(lambda t: t in path, constants.test_categories['tcpt'])):
            target = self.__normalize_tcpt(target=target, path=path)
        return target

    @staticmethod
    def __to_target_name(path):
        return path.replace(constants.local_targets['raw'], constants.local_targets['corrected'])

    @staticmethod
    def __normalize_generic(target):
        return helper.learning.autocorrect(dictionary=target, required=constants.schema, relocate_to='test_keys')

    @staticmethod
    def __normalize_dnst(target, path):
        pprint(target)

        logging.info("Normalising %s as a DNS test" % path)

        target['test_keys'].pop('probe_city')
        target['test_keys'].pop('start_time')
        target['test_keys'].pop('test_resolvers')

        errors = target['test_keys'].pop('tampering')
        target['test_keys']['errors'] = errors
        target['test_keys']['successful'] = set(map(lambda e: e[0], filter(lambda e: e[1] is False, errors.items())))
        target['test_keys']['failed'] = set(map(lambda e: e[0], filter(lambda e: e[1], errors.items())))

        queries = []
        for query in target['test_keys'].pop('queries'):
            query['failure'] = query.get('failure', None)
            query['hostname'] = str(eval(query.pop('query'))[0].name)
            query['resolver_hostname'],  query['resolver_port'] = query.pop('resolver')
            queries.append(query)
        target['test_keys']['queries'] = queries
        return target

    @staticmethod
    def __normalize_httpt(target, path):
        logging.info("Normalising %s as a HTTP test" % path)
        return target

    @staticmethod
    def __normalize_scapyt(target, path):
        logging.info("Normalising %s as a Scapy test" % path)
        return target

    @staticmethod
    def __normalize_tcpt(target, path):
        logging.info("Normalising %s as a TCP test" % path)
        return target


class Sanitise(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return Normalise(self.start_date, self.end_date)

    def run(self):
        bridgedb = self.__load_bridge_db(path=constants.local_targets['bridgedb'], s3_path=constants.s3_targets['bridgedb'])
        for filename in self.input():
            targets = helper.pickles.load(filename.path)
            metrics = list(map(lambda t: self.__sanitize(target=t, bridgedb=bridgedb), targets))
            helper.pickles.save(data=metrics, path=self.__to_target_name(filename.path))

    def output(self):
        return set(map(lambda t: luigi.file.LocalTarget(self.__to_target_name(path=t.path)), self.input()))

    def complete(self):
        outputs = set(filter(lambda x: not os.path.exists(x.path), self.output()))
        return len(outputs) == 0

    @staticmethod
    def __sanitize(target, bridgedb):
        address = target['test_keys'].pop('bridge_address', None)

        if address and address in bridgedb:
            fingerprint = codecs.decode(bridgedb[address]['fingerprint'], 'hex')
            target['test_keys']['distributor'] = bridgedb[address]['distributor']
            target['test_keys']['bridge_hashed_fingerprint'] = hashlib.sha1(fingerprint).hexdigest()
            target['input'] = target['test_keys']['bridge_hashed_fingerprint']
            logging.info("Anonymizing bridge address using BridgeDB")

        for k in ['input', 'test_keys']:
            if target.get(k, None):
                target[k] = Sanitise.__scrub(target=target[k],
                                             redactions=constants.redactions,
                                             except_for={'ipv4_address'})
        return target

    @staticmethod
    def __scrub(target, redactions, except_for=set()):
        if target:
            if except_for:
                redactions = dict(filter(lambda k: k[0] not in except_for, redactions.items()))

            if isinstance(target, str):
                for name, regex in redactions.items():
                    matches = regex.findall(target)
                    if matches:
                        logging.warning("Redacting %s" % name)
                        target = re.sub(regex, constants.redacted, target)

            elif isinstance(target, list):
                target = list(map(lambda x: Sanitise.__scrub(target=x, redactions=redactions), target))

            elif isinstance(target, dict):
                for k, v in target.items():
                    if target.get(k, None):
                        if isinstance(target[k], str):
                            target[k] = Sanitise.__scrub(target=target[k], redactions=redactions)
        return target

    @staticmethod
    def __to_target_name(path):
        return path.replace(constants.local_targets['corrected'], constants.local_targets['sanitised'])

    @staticmethod
    def __load_bridge_db(s3_path, path):
        if not os.path.exists(path):
            helper.s3.get(s3_path=s3_path, local_path=path)
        return helper.jsons.load(path)


def setup():
    for path in constants.local_targets.values():
        if not os.path.exists(path):
            os.mkdir(path)


def cleanup():
    if os.path.exists(Identify.index_file):
        logging.info("Removing S3 key name cache: %s" % Identify.index_file)
        os.remove(Identify.index_file)

if __name__ == '__main__':
    try:
        setup()
        luigi.run()
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()
