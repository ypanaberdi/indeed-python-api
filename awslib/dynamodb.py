from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def del_empty_values(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(d.items()):
        if (value is None or value==""):
            del d[key]
        elif isinstance(value, dict):
            del_empty_values(value)
    return d

def jobExists(job_id):
    dynamodb = boto3.resource("dynamodb", region_name='us-east-1')

    table = dynamodb.Table('jobRecords')

    #job_id = "123456789"

    try:
        response = table.get_item(
            Key={
                'job_id': job_id
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        if 'Item' in response:
            return True

        #item = response['Item']
        #print("GetItem succeeded:")
        #print(json.dumps(item, indent=4, cls=DecimalEncoder))
    return False

def updateJobs(listOfJobs):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('jobRecords')
    unique_jobIds = []
    with table.batch_writer() as batch:
        for j in listOfJobs:
            if j.job_id not in unique_jobIds:
                unique_jobIds.append(j.job_id)

                json_item = del_empty_values({
                    'job_id': j.job_id,
                    'title': j.title,
                    'company': j.company,
                    'city': j.city,
                    'state': j.state,
                    'country': j.country,
                    'source': j.source,
                    'date': j.date,
                    'description': j.description,
                    'url': j.url,
                    'expired': j.expired,
                    'date_published': j.date_published
                })
                batch.put_item(
                    Item=json_item
                )
            else:
                print("skipping job id: {0}".format(j.job_id))
    return