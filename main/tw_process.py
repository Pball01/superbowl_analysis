#!/home/ec2-user/my_app/env/bin/python

import pandas as pd
import boto3
import json
from my_aws_secrets import s3_access_key_id, s3_secret_access_key, s3_bucket
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


s3_client = boto3.client(
    "s3",
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_secret_access_key,
)


if not os.path.exists("preprocessed"):
    os.makedirs("preprocessed")


def download_csv_to_pandas_from_s3(path):
    # get csv from s3 into memory
    response = s3_client.get_object(Bucket=s3_bucket, Key=path)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Key={path}")
        df = pd.read_csv(response.get("Body"), lineterminator="\n")
        return df
    else:
        raise Exception(f"Unsuccessful S3 get_object response. Status - {status}")

def upload_processed_csv_to_s3(csv_path):
    with open(csv_path, "rb") as f:
        response = s3_client.put_object(Bucket=s3_bucket, Key="twitter/preprocessed/twitter.csv", Body=f)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Key=twitter/preprocessed/twitter.csv")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")


#  Read each csv into a data frame and return it
def get_dfs(list_of_csv_paths):
    for path in list_of_csv_paths:
        # Identify the brand and directory from the file path name (ex: raw_data/after/uber eats.csv)
        _, brand = path.split("/")[-1][:-4].split("_")
        directory = path.split("/")[2]
        # get csv from s3 into pandas
        df = download_csv_to_pandas_from_s3(path)
        # add the brand and directory columns to the data frame
        df["brand"] = brand
        df["directory"] = directory
        df = df.reset_index(drop=True)
        yield df


sid = SentimentIntensityAnalyzer()
# calculate the sentiment for each tweet
def calculate_sentiments(df):
    df["scores"] = df["Text"].apply(lambda Text: sid.polarity_scores(Text))
    df["compound"] = df["scores"].apply(lambda score_dict: score_dict["compound"])
    df["comp_score"] = df["compound"].apply(
        lambda c: "neu" if c == 0 else ("pos" if c > 0 else "neg")
    )
    del df["scores"]
    df = df.drop_duplicates(subset=["Tweet Id"])
    return df


# add a column for if the tweet contains the word "superbowl"
def check_superbowl_keyword(df):
    df["contains_superbowl"] = df["Text"].apply(lambda x: "superbowl" in x.lower())
    return df


def preprocess_twitter():
    OUT_PATH = "preprocessed/twitter.csv"

    with open("companies_test.json", "r") as f:
        brands = json.load(f)
    dir_list = []
    for timeframe in ["before", "after"]:
        for brand, years in brands.items():
            for year in years:
                dir_list.append(f"twitter/raw_data/{timeframe}/{year}_{brand}.csv")

    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    for df in get_dfs(dir_list):
        # process individual dataframe
        df = calculate_sentiments(df)
        #  determine if superbowl keyword is included
        df = check_superbowl_keyword(df)
        # write processed dataframe to output file
        mode, header = ("w", True) if not os.path.exists(OUT_PATH) else ("a", False)
        df.to_csv(
            OUT_PATH,
            mode=mode,
            header=header,
            index=False
        ) 
    
    upload_processed_csv_to_s3(OUT_PATH)
    os.remove(OUT_PATH)

if __name__ == '__main__':
    preprocess_twitter()