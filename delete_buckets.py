import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def set_resource():
    s3 = boto3.resource('s3', aws_access_key_id=os.environ.get('access_key'),
              aws_secret_access_key=os.environ.get('secret_key'),
              region_name=os.environ.get('region'))
    return s3

def get_s3_buckets():
    s3 = set_resource()
    buckets = s3.buckets.all()
    return [bucket.name for bucket in buckets]


def delete_bucket(bucket, s3=None):
    if s3:
        pass
    else:
        s3 = set_resource()
    s3.Bucket(bucket).delete()


buckets_to_skip = ['sdh-customerbkt-27', 'sdh-customerbkt-17', 'sdh-customerbkt-23', 'sdh-customerbkt-26',
                   'sdh-customerbkt-24', 'sdh-customerbkt-18', 'sdh-customerbkt-25']
delete_buckets = [bucket for bucket in get_s3_buckets() if 'sdh-customerbkt' in bucket]
for i in delete_buckets:
    if i not in buckets_to_skip:
        print(i)
        delete_bucket(i)
