import os
import re

ignore_file = 'ignore.pickle'

redacted = '█████'

s3_targets = {
    'raw': 's3://ooni-private/reports-raw/yaml',
    'bridgedb': 's3://ooni-private/bridge_reachability/bridge_db.json',
}

local_targets = {
    'raw': 'targets/raw',
    'corrected': 'targets/corrected',
    'sanitised': 'targets/sanitised',
    'bridgedb': os.path.basename(s3_targets['bridgedb'])
}

redactions = {
    'password': re.compile('password=[\S]+'),
    'bridge_fingerprint': re.compile('[A-F0-9]{40}'),
    'ipv4_address': re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),
    'ipv4_socket': re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}')
}

credentials = {
    'aws_access_key_id': os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['AWS_SECRET_ACCESS_KEY'],
}

schema = set([
    'input',
    'report_id',
    'report_filename',
    'options',
    'probe_cc',
    'probe_asn',
    'probe_ip',
    'data_format_version',
    'test_name',
    'test_start_time',
    'test_runtime',
    'test_helpers',
    'test_keys',
    'software_name',
    'software_version',
    'test_version',
])