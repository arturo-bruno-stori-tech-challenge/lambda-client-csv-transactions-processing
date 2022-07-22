import os
import csv
import json
import logging
import urllib.parse
from pathlib import Path

import boto3

print('Loading function')

s3 = boto3.client('s3')

# rds settings
rds_host = os.environ.get('RDS_HOST')
rds_username = os.environ.get('RDS_USERNAME')
rds_user_pwd = os.environ.get('RDS_USER_PWD')
rds_db_name = os.environ.get('RDS_DB_NAME')

# try:
#     conn = psycopg2.connect(
#         host=rds_host,
#         user=rds_username,
#         password=rds_user_pwd
#     )
# except:
#     logging.error("ERROR: Could not connect to Postgres instance.")
# else:
#     logging.info("SUCCESS: Connection to RDS Postgres instance succeeded")


def move_s3_file(bucket, file, destination):
    s3.copy_object(
        Bucket=bucket,
        CopySource=file,
        Key=destination
    )

    s3.delete_object(
        Bucket=bucket,
        Key=file
    )


def lambda_handler(event, context):
    print(f'Received event: {json.dumps(event, indent=2)}')
    print(f'host: "{rds_host}"')
    print(f'user: "{rds_username}"')

    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(filename, encoding='utf-8')

    client_name = Path(filename).stem.replace('_', ' ')
    print(f'Client: "{client_name}"')
    print(f'Bucket: "{bucket}"')
    print(f'Key: "{key}"')

    try:
        csv_file = s3.get_object(Bucket=bucket, Key=key)['Body'].read().splitlines()
        print('csv_file', csv_file)
    except Exception as e:
        print(f'Error getting object {key} from bucket {bucket}: "{e}"')
        raise e

    client_transactions = csv.DictReader(csv_file)

    # Move CSV file to "processing" folder
    print('key', key)
    # move_s3_file(bucket, key)

    # Process the CSV file
    for transaction in client_transactions:
        print(transaction)
        # Save each transaction to the database to the specific client

    # Move the CSV file to "processed" folder

    # Send event to trigger



Received event: {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-2",
            "eventTime": "2022-07-22T01:22:03.522Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "A1T8W4UQT29LXO"
            },
            "requestParameters": {
                "sourceIPAddress": "189.203.6.28"
            },
            "responseElements": {
                "x-amz-request-id": "1ZSF00B7VA4K4ZCN",
                "x-amz-id-2": "/8PtAqbaorEEe4A8YKw4n4rAJWi4FbidDeqodOy/CX44Js+eYR3kvgwEt0hTMMLHlClE1I1kVvdTvfXzCOTme6lhnR/MYLN+"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "71419a86-41a0-4453-8f8a-cb69875a9ae8",
                "bucket": {
                    "name": "clients-transactions",
                    "ownerIdentity": {
                        "principalId": "A1T8W4UQT29LXO"
                    },
                    "arn": "arn:aws:s3:::clients-transactions"
                },
                "object": {
                    "key": "unprocessed/Todd_Long.csv",
                    "size": 181324,
                    "eTag": "5b895878b974402cc94f96b10ac570e6",
                    "versionId": "Os_4gEYXcOiG4jKxwE6UIg5dXqGaFLIE",
                    "sequencer": "0062D9FBBB721E0C50"
                }
            }
        }
    ]
}

host: "database-1-instance-1.csf7zau4mboa.us-east-2.rds.amazonaws.com"
user: "summary"