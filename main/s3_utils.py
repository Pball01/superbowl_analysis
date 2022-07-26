import io
import pandas as pd
import boto3
from my_aws_secrets import s3_access_key_id, s3_secret_access_key, s3_bucket

s3_client = boto3.client(
    "s3",
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_secret_access_key,
)

def get_pandas_df(key, **kwargs):
    response = s3_client.get_object(Bucket=s3_bucket, Key=key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Key={key}")
        df = pd.read_csv(response.get("Body"), **kwargs)
        return df
    else:
        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

def put_pandas_df(key, df, **kwargs):
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, **kwargs)

        response = s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())   
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Key={key}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")

def get_file_as_string(key):
    response = s3_client.get_object(Bucket=s3_bucket, Key=key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Key={key}")
        string = response.get("Body").read().decode('utf-8')
        return string
    else:
        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")