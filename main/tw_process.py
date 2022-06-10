import pandas as pd
import snscrape.modules.twitter as sntwitter
from datetime import datetime
from datetime import timedelta
import glob
import os
import numpy as np
import re
import pickle
#import spacy
#import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
#import ssl


#uploading company name, use companies_test for shorter data, use companies for all data
with open('companies_test.pkl', 'rb') as f:
    brands = pickle.load(f)

companies= list(brands.keys())#only brand names from dictionary

if not os.path.exists("preprocessed"):
    os.makedirs("preprocessed")



sid = SentimentIntensityAnalyzer()

#joining files
def prep_concat(filenames):
    for files in filenames:
        for file in files:
            df = pd.read_csv(file, lineterminator='\n')
            directory = file.partition("/")[2].partition("/")[0]
            company_name = file.partition("_")[2].partition(".")[0]
            df['Company_Name'] = company_name
            df['Directory'] = directory
            df = df.reset_index(drop=True)
            yield df



def calculate_sentiments(df):
    df['scores'] = df['Text'].apply(lambda Text: sid.polarity_scores(Text))
    df['compound']  = df['scores'].apply(lambda score_dict: score_dict['compound'])
    df['comp_score'] = df['compound'].apply(lambda c: 'neu' if c ==0 else ('pos' if c>0 else 'neg'))
    del df['Rendered Content']
    del df['scores']
    df = df.drop_duplicates(subset = ['Tweet Id'])
    return df




#list of filenames
filenames_before = sorted(glob.glob(f'raw_data/before/*{company}.csv') for company in companies)
filenames_after = sorted(glob.glob(f'raw_data/after/*{company}.csv') for company in companies)
filenames_after_superbowl = sorted(glob.glob(f'raw_data/after/superbowl/*{company}.csv') for company in companies)






if __name__ == '__main__':
    #creating bigger files
    before_df = pd.concat((prep_concat(filenames_before))).reset_index(drop=True)
    after_df = pd.concat((prep_concat(filenames_after))).reset_index(drop=True)
    after_superbowl_df = pd.concat((prep_concat(filenames_after_superbowl))).reset_index(drop=True)

    #calculate sentiments
    before_df = calculate_sentiments(before_df)
    after_df = calculate_sentiments(after_df)
    after_superbowl_df = calculate_sentiments(after_superbowl_df)

    before_df.to_csv(r'preprocessed/before_df.csv')
    after_df.to_csv(r'preprocessed/after_df.csv')
    after_superbowl_df.to_csv(r'preprocessed/after_superbowl_df.csv')
