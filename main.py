import os
import csv
import json
import pymysql
from pathlib import Path
from datetime import date, datetime

import boto3

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

rds_host = os.getenv('RDS_HOST')
rds_username = os.getenv('RDS_USERNAME')
rds_password = os.getenv('RDS_PASSWORD')
rds_database = os.getenv('RDS_DATABASE')

try:
    db = pymysql.connect(
        host=rds_host,
        user=rds_username,
        passwd=rds_password,
        db=rds_database,
        connect_timeout=5,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
except pymysql.MySQLError as e:
    print(f'ERROR: Unexpected error: Could not connect to MySQL instance: "{e}"')
    exit(1)


def move_s3_file(bucket, file, destination):
    s3_action = None
    print(f'Moving "{file}" to "{destination}"')
    try:
        s3_resource.Object(bucket, destination).copy_from(CopySource=f'{bucket}/{file}')
        s3_resource.Object(bucket, file).delete()
        return destination
    except Exception as e:
        print(f'Could not move "{file}" to "{destination}": "{e}" | s3_action: "{s3_action}"')
        exit(1)


def get_client(client_name: str):
    print(f'Looking for client: "{client_name}"')
    with db.cursor() as cursor:
        cursor.execute('SELECT * FROM clients WHERE name LIKE %s', f'%{client_name}%')
        if client := cursor.fetchone():
            print(f'Client found! ID: "{client["id"]}"')
            return client
    return None


def create_client(client_name: str):
    print(f'Creating client: "{client_name}"')
    try:
        with db.cursor() as cursor:
            cursor.execute('INSERT INTO clients(name) VALUES(%s)', client_name)
            db.commit()
            print('Client successfully created!')

            return get_client(client_name)
    except Exception as exception:
        print(f'Could not create client "{client_name}": "{exception}"')
        exit(1)


def save_transactions(client: dict, client_transactions: list):
    print(f'Saving {len(client_transactions)} transactions for client "{client}"')
    try:
        transactions = []
        for transaction in client_transactions:
            transactions.append((
                client['id'],
                int(transaction['Id']),
                str(parse_transaction_date(transaction['Date'])),
                transaction['Transaction']
            ))

        with db.cursor() as cursor:
            cursor.executemany(
                '''
                    INSERT IGNORE INTO transactions(client_id, transaction_id, date, amount) 
                    VALUES(%s, %s, %s, %s)
                ''',
                transactions
            )
            db.commit()
            print('Transactions saved successfully!')
    except Exception as exception:
        print(f'Could not create client transactions "{client}": "{exception}"')
        exit(1)


def parse_transaction_date(transaction_date: str) -> date:
    trans_date = transaction_date.split('/')
    return datetime(datetime.now().year, int(trans_date[0]), int(trans_date[1])).date()


def lambda_handler(event, context):
    print(f'Received event: {json.dumps(event)}')

    filename = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    client_name = Path(filename).stem.replace('_', ' ')
    csv_file = s3.get_object(Bucket=bucket, Key=filename)['Body'].read().decode('utf-8').splitlines()
    print(f'Client: "{client_name}"')
    print(f'Bucket: "{bucket}"')
    print(f'File: "{filename}"')

    client_transactions = csv.DictReader(csv_file)
    transactions = [transaction for transaction in client_transactions]
    print(f'Client "{client_name}" transactions CSV "{filename}" has "{len(transactions)}" transactions')

    if (client := get_client(client_name)) is None:
        client = create_client(client_name)

    # Move CSV file to "processing" folder
    processing_filename = move_s3_file(bucket, filename, filename.replace('unprocessed', 'processing'))

    # Save each transaction to the database to the specific client
    save_transactions(client, transactions)

    # Move the CSV file to "processed" folder
    processed_filename = move_s3_file(
        bucket,
        processing_filename,
        processing_filename.replace('processing', 'processed')
    )

    # Send event to trigger

    # Finish function successfully
    message = f'Client "{client_name}" CSV transactions file "{bucket}.{processed_filename}". Processed successfully'
    return {
        'message': message
    }


if __name__ == '__main__':
    lambda_handler({}, None)
