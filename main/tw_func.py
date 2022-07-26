import re
from datetime import datetime, timedelta

import pandas as pd
import snscrape.modules.twitter as sntwitter
from tqdm import tqdm
import boto3
from my_aws_secrets import s3_access_key_id, s3_secret_access_key,s3_bucket
import io
import os

# create a client to access resources with
s3_client = boto3.client(
    's3', # add the service name we are accessing
    aws_access_key_id = s3_access_key_id, #update this
    aws_secret_access_key = s3_secret_access_key #update this
)



superbowl_dates = [
    "2015-02-01",
    "2016-02-07",
    "2017-02-05",
    "2018-02-04",
    "2019-02-03",
    "2020-02-02",
    "2021-02-07",
    "2022-02-13",
]
dates_list = [
    datetime.strptime(date, "%Y-%m-%d").date() for date in superbowl_dates
]  # turning dates to datetime format
before_date = [
    str(date - timedelta(days=2)) for date in dates_list
]  # calculating dates that is 2 days before superbowl
after_date = [
    str(date + timedelta(days=2)) for date in dates_list
]  # calculating dates that is 2 days after superbowl


def get_brand_tweets_in_year_before(brand, year):
    """
    Function to download data for days before superbowl for each brand during different years
    Data is downloaded in before folder
    """

    tweet: sntwitter.Tweet
    # Creating list to append tweet data to
    tweets_list2 = []

    # Using TwitterSearchScraper to scrape data and append tweets to list

    # calculating date from
    year_match_since = [s for s in before_date if str(year) in s]
    since = ",".join(year_match_since)

    # calculating date to
    year_match_after = [s for s in superbowl_dates if str(year) in s]
    until = ",".join(year_match_after)

    # create progress bar
    pbar = tqdm(desc=f"{brand} {year} Before", total=750)
    for i, tweet in enumerate(
        sntwitter.TwitterSearchScraper(
            f"{brand} since:{since} until:{until} lang:en filter:has_engagement"
        ).get_items()
    ):
        # filtering for specific time range, tweets are in english and tweets have engagements

        if tweet.retweetCount > 0:
            tweets_list2.append(
                [
                    tweet.url,
                    tweet.date,
                    tweet.id,
                    re.sub(r"[\r\n]+", " ", tweet.content),
                    tweet.retweetCount,
                    tweet.replyCount,
                    tweet.likeCount,
                    tweet.user.username,
                    tweet.user.displayname,
                    tweet.user.followersCount,
                    tweet.user.friendsCount,
                ]
            )
            pbar.update(1)
        if (
            len(tweets_list2) == 750
        ):  # pulling 750 tweets max for each brand for each year
            break
    pbar.close()

    # Creating a dataframe from the tweets list above
    tweets_df2 = pd.DataFrame(
        tweets_list2,
        columns=[
            "url",
            "Datetime",
            "Tweet Id",
            "Text",
            "Retweet Count",
            "Reply Count",
            "Like Count",
            "Username",
            "Display Name",
            "Followers Count",
            "Friends Count",
        ],
    )

    tweets_df2["Datetime"] = pd.to_datetime(tweets_df2["Datetime"])
    tweets_df2["Day"] = tweets_df2["Datetime"].dt.day
    tweets_df2["Month"] = tweets_df2["Datetime"].dt.month
    tweets_df2["Year"] = tweets_df2["Datetime"].dt.year

    # Write df to a csv file
    with io.StringIO() as csv_buffer:
        tweets_df2.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket= "superbowl-test", Key=f"twitter/raw_data/before/{year}_{brand}.csv", Body=csv_buffer.getvalue()
        )   

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")


    # tweets_df2.to_csv(f"raw_data/before/{year}_{brand}.csv", index=False, header=True)




def get_brand_tweets_in_year_after(brand, year):
    """
    Function to download data for days after superbowl for each brand during different years
    Data is downloaded in after folder
    """

    tweet: sntwitter.Tweet
    # Creating list to append tweet data to
    tweets_list2 = []

    year_match_since = [s for s in superbowl_dates if str(year) in s]
    since = ",".join(year_match_since)

    year_match_after = [s for s in after_date if str(year) in s]
    until = ",".join(year_match_after)

    # create progress bar
    pbar = tqdm(desc=f"{brand} {year} After", total=750)
    for i, tweet in enumerate(
        sntwitter.TwitterSearchScraper(
            f"{brand} since:{since} until:{until} lang:en filter:has_engagement"
        ).get_items()
    ):
        if tweet.retweetCount > 0:
            tweets_list2.append(
                [
                    tweet.url,
                    tweet.date,
                    tweet.id,
                    re.sub(r"[\r\n]+", " ", tweet.content),
                    tweet.retweetCount,
                    tweet.replyCount,
                    tweet.likeCount,
                    tweet.user.username,
                    tweet.user.displayname,
                    tweet.user.followersCount,
                    tweet.user.friendsCount,
                ]
            )
            pbar.update(1)
        if len(tweets_list2) == 750:
            break
    pbar.close()

    # Creating a dataframe from the tweets list above
    tweets_df2 = pd.DataFrame(
        tweets_list2,
        columns=[
            "url",
            "Datetime",
            "Tweet Id",
            "Text",
            "Retweet Count",
            "Reply Count",
            "Like Count",
            "Username",
            "Display Name",
            "Followers Count",
            "Friends Count",
        ],
    )

    tweets_df2["Datetime"] = pd.to_datetime(tweets_df2["Datetime"])
    tweets_df2["Day"] = tweets_df2["Datetime"].dt.day
    tweets_df2["Month"] = tweets_df2["Datetime"].dt.month
    tweets_df2["Year"] = tweets_df2["Datetime"].dt.year

        # Write df to a csv file
    with io.StringIO() as csv_buffer:
        tweets_df2.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket= "superbowl-test", Key=f"twitter/raw_data/after/{year}_{brand}.csv", Body=csv_buffer.getvalue()
        )   

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")



    # Write df to a csv file
    # tweets_df2.to_csv(f"raw_data/after/{year}_{brand}.csv", index=False, header=True)


