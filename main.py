import csv
import json
import logging
import urllib.parse
from pathlib import Path

import boto3

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    logging.info(f'Received event: {json.dumps(event, indent=2)}')

    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(filename, encoding='utf-8')

    try:
        s3_object = s3.Object(bucket, key)
        csv_data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        print(f'Error getting object {key} from bucket {bucket}: "{e}"')
        raise e

    client_transactions = csv.DictReader(csv_data)
    client_name = Path(filename).stem.replace('_', ' ')
    print(f'Client: "{client_name}"')

    for transaction in client_transactions:
        print(transaction)
        print()
