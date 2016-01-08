import collections
import os

ooni_s3_targets = {}

ooni_s3_targets['raw'] = {
    'bucket': 'ooni-private',
    'prefix': 'reports-raw/yaml'
}

credentials = {
    'aws_access_key_id' : os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key' : os.environ['AWS_SECRET_ACCESS_KEY'],
}