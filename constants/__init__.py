import os

ooni_s3_targets = {
    'raw': 's3://ooni-private/reports-raw/yaml'
}

credentials = {
    'aws_access_key_id' : os.environ['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key' : os.environ['AWS_SECRET_ACCESS_KEY'],
}