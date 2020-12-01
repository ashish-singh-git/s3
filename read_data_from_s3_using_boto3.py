#Talk python to me
import json
import logging
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None):
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True



# Retrieve the list of existing buckets
response = create_bucket('ashish-test-2020','us-west-2')
print(response)

# Retrieve the list of existing buckets
s3 = boto3.client('s3')
response = s3.list_buckets()

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

# upload file to S3 bucket
bucket = 'ashish-test-2020'
key = 'data.json'
with open('data.json', 'rb') as f:
    s3.upload_fileobj(f, bucket, key)

# Read the file from S3 bucket
response = s3.get_object(Bucket=bucket, Key=key)
content = response['Body']
jsonObject = json.loads(content.read())
transactions = jsonObject['transactions']

print(response)
print("---")
print("---")
print(content)
print("---")
print("---")
print(jsonObject)

for record in transactions:
   print("TransactionType: " + record['transactionType'])
   print("TransactionAmount: " + str(record['amount']))
   print("---")


