import boto3
import botocore
import logging
import io
import os
import pandas
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# logging.basicConfig(filename='logs/AWS_' + datetime.now().strftime('%Y-%m-%d') + '.txt',
#                     level=logging.INFO,
#                     format="%(asctime)s:%(levelname)s:%(lineno)d:%(message)s"
#                     )

class S3:
    def __init__(self, bucket_name):
        self.access_key = os.environ['access_key']
        self.secret_key = os.environ['secret_key']
        self.region = os.environ['region']
        self.bucket_name = bucket_name


    def get_signed_url(self, file_name):
        # endpoint = 'https://{}.s3-website.{}.amazonaws.com/{}'.format(self.bucket_name, self.region, file_name)
        endpoint = 'https://{}.s3.{}.amazonaws.com/{}'.format(self.bucket_name, self.region, file_name)

        """ Needed to add both botocore client explicitly stating the s3 version to use
            and the region due to the call defaulting to us-east-2 found at the following link
            https://github.com/boto/boto3/issues/1149
        """
        s3 = boto3.client('s3',
                          # endpoint_url=endpoint,
                          config=botocore.client.Config(signature_version='s3v4'),
                          aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key,
                          region_name=self.region)
        # signed_url = s3.generate_presigned_post(Bucket=self.bucket_name, Key=file_name, ExpiresIn=3600)

        signed_url = s3.generate_presigned_url(
            ClientMethod='put_object',
            Params={'Bucket': self.bucket_name, 'Key': file_name},
            ExpiresIn=3600
            )
        return signed_url

    def create_bucket(self):
        try:
            s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.secret_key,
                              region_name=self.region)

            s3.create_bucket(Bucket=self.bucket_name)
            self.set_bucket_cors_policy()
            status = {"status": 1, "bucket": self.bucket_name}
            logging.info('{} bucket created'.format(self.bucket_name))
        except Exception as e:
            logging.error('AWS Create Bucket - {}'.format(str(e)))
            msg = '{} - Manual Entry'.format(datetime.now().strftime('%Y-m-d '))
            status = {"status": -1, "error_msg": msg}
        finally:
            return status

    def get_header_csv(self, file_name):
        s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key,
                          region_name=self.region)
        obj = s3.get_object(Bucket=self.bucket_name, Key=file_name)
        byte_read_as_string = obj['Body'].read().decode('utf-8').splitlines()
        survey_data = pandas.read_csv(io.StringIO(byte_read_as_string[0]))
        header = survey_data.columns.values.tolist()
        result = {"status": 0, "columns": header}
        return result

    def set_bucket_cors_policy(self):
        cors_configuration = {
            'AllowedHeaders': ['*'],
            'AllowedMethods': ['GET', 'PUT'],
            'AllowedOrigins': ['*'],
            'MaxAgeSeconds': 3000

        }
        s3 = self.set_s3_client()
        s3.put_bucket_cors(Bucket=self.bucket_name,
                           CORSConfiguration=cors_configuration)


    def create_json_file(self, file_name, body):
        try:
            s3 = self.set_s3_resource()
            file_name = file_name + '.json'
            s3.Object(self.bucket_name, file_name).put(Body=json.dumps(body))
            return {"status": 0, "msg": 'complete'}
        except Exception as e:
            return {"status": -1, "msg": str(e)}

    def download_file(self, file_name, save_path):
        try:
            s3 = self.set_s3_client()
            s3.download_file(self.bucket_name, file_name, save_path)
            return {"status": 0, "msg": 'complete'}
        except Exception as e:
            return {"status": -1, "msg": str(e)}

    def upload_file(self, file_path, key_name, meta_data: dict):
        try:
            s3 = self.set_s3_client()
            s3.upload_file(file_path, self.bucket_name, key_name, ExtraArgs={"Metadata":meta_data})
        except Exception as e:
            print(e)


    def set_s3_client(self):
        s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key,
                          region_name=self.region)
        return s3

    def set_s3_resource(self):
        s3 = boto3.resource('s3', aws_access_key_id=self.access_key,
                          aws_secret_access_key=self.secret_key,
                          region_name=self.region)
        return s3



class SQS:
    def __init__(self):
        self.access_key = os.environ['access_key']
        self.secret_key = os.environ['secret_key']
        self.region = os.environ['region']