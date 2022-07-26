#!/home/ec2-user/my_app/env/bin/python
import json
import time
import boto3
from my_aws_secrets import (
    redshift_access_key_id,
    redshift_secret_access_key,
    redshift_region_name,
    redshift_cluster_name,
    redshift_db_name,
    redshift_db_user,
)

# Read create_schema.sql from S3, which contains the schema definition

client = boto3.client(
    "redshift-data",
    aws_access_key_id=redshift_access_key_id,
    aws_secret_access_key=redshift_secret_access_key,
    region_name=redshift_region_name
)

def copy_data():
    with open("copy_data_to_redshift.sql", "r") as f:
        sqlFile = f.read()
        sql_statements = [s.strip() for s in sqlFile.split(";") if len(s.strip()) > 0]

    response = client.batch_execute_statement(
        ClusterIdentifier=redshift_cluster_name,
        Database=redshift_db_name,
        DbUser=redshift_db_user,
        Sqls=sql_statements,
        StatementName="Copy Data",
    )
    describe = client.describe_statement(Id=response['Id'])
    while describe['Status'] not in ['FAILED', 'FINISHED']:
        print("Status - " + describe['Status'] + " waiting 15 seconds")
        time.sleep(15)
        describe = client.describe_statement(Id=response['Id'])
    
    if describe['Status'] == 'FINISHED':
        print("Successfully copied data to redshift")
    else:
        print("Failed to copy data to redshift")
        print(describe['Error'])
    
    # print(response)
    # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    # if status == 200:
    #     print(f"Successful S3 get_object response. Status - {status}")
    # else:
    #     raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")


if __name__ == "__main__":
    copy_data()
