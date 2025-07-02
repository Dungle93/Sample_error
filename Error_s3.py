import boto3
import pandas as pd

def process_data(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])

    filtered = df[df['amount'] > 1000]

    s3.put_object(Bucket=bucket, Key='filtered_data.csv', Body=filtered.to_csv(index=False))

process_data('my-bucket', 'data/sales.csv')
