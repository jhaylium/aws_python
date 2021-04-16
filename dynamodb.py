import boto3, os, json
from decimal import Decimal
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key

load_dotenv()
def set_resource():
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=os.environ.get('access_key'),
              aws_secret_access_key=os.environ.get('secret_key'),
              region_name=os.environ.get('region'))
    return dynamodb


def insert_record(dynamodb, table_name, payload):
    """Parse Float is used to convert decimals to float as floats are not supported in the python sdk"""
    table = dynamodb.Table(table_name)
    payload = json.loads(json.dumps(payload), parse_float=Decimal)
    response = table.put_item(Item=payload)
    return response

def get_item(dynamodb, table_name, key_field_name, key_value):
    table = dynamodb.Table(table_name)
    resp = table.get_item(
        Key={
            key_field_name: key_value,
        }
    )
    if 'Item' in resp:
        return (0, resp['Item'])
    else:
        return (-1, resp)


def delete_record(dynamodb, table_name, key_field_name, key_value):
    table = dynamodb.Table(table_name)
    response = table.delete_item(
        Key={
            key_field_name: key_value,
        },
    )
    return response

def update_record(dynamodb, table_name, key_field_name, key_value, update_field, update_value):
    """This is not meant to use compound keys. It is trivial to fix this."""
    table = dynamodb.Table(table_name)
    table.update_item(
        Key={
            key_field_name: key_value
        },
        UpdateExpression=f'SET {update_field} = :val1',
        ExpressionAttributeValues={
            ':val1': {update_value}
        }
    )

def batch_insert_record():
    """This allows you to insert more than one record at a time and reduces the number of writes,
     thus saving us money """
    pass

table = "topics"
kf = "project_id"
kv = 24
dydb = set_resource()
document = get_item(dynamodb=dydb,
         table_name=table,
         key_field_name=kf,
         key_value=kv)

print(document)