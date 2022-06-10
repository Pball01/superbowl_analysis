import pandas as pd
import snscrape.modules.twitter as sntwitter
from datetime import datetime
from datetime import timedelta
import glob
import os
import numpy as np
import re
import pickle
from tw_func import get_brand_tweets_in_year_after, get_brand_tweets_in_year_after_sb, get_brand_tweets_in_year_before


#uploading company name, use companies_test for shorter data, use companies for all data
with open('companies_test.pkl', 'rb') as f:
    brands = pickle.load(f)



def create_raw_folders():
#creating folder
    if not os.path.exists("raw_data"):
        os.makedirs("raw_data")

    if not os.path.exists("raw_data/before"):
        os.makedirs("raw_data/before")

    if not os.path.exists("raw_data/after"):
        os.makedirs("raw_data/after")

    if not os.path.exists("raw_data/after/superbowl"):
        os.makedirs("raw_data/after/superbowl")

#pulling twitter data
def pull_twitter_data():
    for brand, years in brands.items():
        for year in years:
            get_brand_tweets_in_year_before(brand, year) 
    for brand, years in brands.items():
        for year in years:
            get_brand_tweets_in_year_after(brand, year)
    for brand, years in brands.items():
        for year in years:
            get_brand_tweets_in_year_after_sb(brand, year) 


if __name__ == '__main__':
    create_raw_folders()
    pull_twitter_data()











