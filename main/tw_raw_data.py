#!/home/ec2-user/my_app/env/bin/python

import json
import os

from tqdm import tqdm

from tw_func import get_brand_tweets_in_year_after, get_brand_tweets_in_year_before

# uploading company name, use companies_test for shorter data, use companies for all data
with open("companies_test.json", "r") as f:
    brands = json.load(f)



def create_raw_folders():
    # creating folder
    if not os.path.exists("raw_data"):
        os.makedirs("raw_data")

    if not os.path.exists("raw_data/before"):
        os.makedirs("raw_data/before")

    if not os.path.exists("raw_data/after"):
        os.makedirs("raw_data/after")




# pulling twitter data
def pull_twitter_data():
    pbar = tqdm(
        desc="Files Created", total=sum(len(years) for years in brands.values()) * 2
    )
    for brand, years in brands.items():
        for year in years:
            get_brand_tweets_in_year_before(brand, year)
            pbar.update(1)
    for brand, years in brands.items():
        for year in years:
            get_brand_tweets_in_year_after(brand, year)
            pbar.update(1)
    pbar.close()


if __name__ == "__main__":
   create_raw_folders()
   pull_twitter_data()
