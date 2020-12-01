import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event, context):
    #1 - Get the bucket name
    bucket = event['Records'][0]['s3']['bucket']['name']

    #2 - Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        #3 - Fetch the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)

        #4 - Deserialize the file's content
        text = response["Body"].read().decode()
        data = json.loads(text)

        #5 - Print the content
        print(data)

        #6 - Parse and print the transactions
        transactions = data['transactions']
        for record in transactions:
            print(record['transactionType'])
        return 'Success!'

    except Exception as e:
        print(e)
        raise e