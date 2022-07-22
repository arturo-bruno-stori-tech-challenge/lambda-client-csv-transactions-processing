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
        s3_object = s3.get_object(Bucket=bucket, Key=key)
        print('s3_object', s3_object)

        s3_body = s3_object['Body']
        print('s3_body', s3_body)

        csv_file = s3_body.read().decode('utf-8').splitlines()
        print('csv_file', csv_file)
    except Exception as e:
        print('ERROR:', e)
        # print(f'Error getting object {key} from bucket {bucket}: "{e}"')
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
