# lambda-client-csv-transactions-processing
Handle the S3 events when receiving a new client CSV transaction file

* Receive the S3 events when a new CSV is uploaded
* Read de CSV from S3 to create the client (if not already on the database)
* Save the transactions into the database

## Environment variables needed
 * RDS_HOST
 * RDS_USERNAME
 * RDS_PASSWORD
 * RDS_DATABASE
 * AWS_REGION
 * CLIENTS_FAKE_EMAIL
 * SUMMARY_NOTIFICATION_TOPIC

## Tables needed in database
* clients
  * id
  * name
  * email
* transactions
  * id
  * client_id
  * transaction_id
  * date
  * amount

