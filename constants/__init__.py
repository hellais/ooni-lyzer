import os
import re

ignore_file = 'ignore.pickle'

ooni_s3_targets = {}
ooni_s3_targets['raw'] = 's3://ooni-private/reports-raw/yaml'
ooni_s3_targets['bridgedb'] = 's3://ooni-private/bridge_reachability/bridge_db.json'

local_targets = {}
local_targets['raw'] = 'targets/raw'
local_targets['corrected'] = 'targets/corrected'
local_targets['sanitised'] = 'targets/sanitised'

regular_expressions = {
    'tor_log': {
        'bridge_fingerprint': re.compile('[A-F0-9]{40}'),
        'ipv4_address': re.compile('(\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(:\d{1,5})?\b'),
    }
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