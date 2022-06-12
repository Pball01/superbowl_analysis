import pandas as pd
import glob
import os
import pickle
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

OUT_PATH = 'preprocessed/twitter.csv'

#uploading company name, use companies_test for shorter data, use companies for all data
with open('companies_test.pkl', 'rb') as f:
    brands = pickle.load(f)

companies= list(brands.keys())#only brand names from dictionary

if not os.path.exists("preprocessed"):
    os.makedirs("preprocessed")


sid = SentimentIntensityAnalyzer()


#  Read each csv into a data frame and return it
def get_dfs(list_of_csv_paths):
    for path in list_of_csv_paths:
        # Identify the brand and directory from the file path name (ex: raw_data/after/uber eats.csv)
        _, brand = path.split('/')[-1][:-4].split('_')
        directory = path.split('/')[1]
        # Convert the csv into a data frame
        df = pd.read_csv(path, lineterminator='\n')
        # add the brand and directory columns to the data frame
        df['brand'] = brand
        df['directory'] = directory
        df = df.reset_index(drop=True)
        yield df


# calculate the sentiment for each tweet
def calculate_sentiments(df):
    df['scores'] = df['Text'].apply(lambda Text: sid.polarity_scores(Text))
    df['compound']  = df['scores'].apply(lambda score_dict: score_dict['compound'])
    df['comp_score'] = df['compound'].apply(lambda c: 'neu' if c ==0 else ('pos' if c>0 else 'neg'))
    del df['Rendered Content']
    del df['scores']
    df = df.drop_duplicates(subset = ['Tweet Id'])
    return df


# add a column for if the tweet contains the word "superbowl"
def check_superbowl_keyword(df):
    df['contains_superbowl'] = df['Text'].apply(lambda x: 'superbowl' in x.lower())
    return df


if __name__ == '__main__':

    dir_list = []
    for timeframe in ['before', 'after']:
        for path in glob.glob(f'raw_data/{timeframe}/*.csv'):
            dir_list.append(path)

    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)
    

    for df in get_dfs(dir_list):
        # process individual dataframe
        df = calculate_sentiments(df)
        #  determine if superbowl keyword is included
        df = check_superbowl_keyword(df)
        # write processed dataframe to output file
        mode, header = ('w', True) if not os.path.exists(OUT_PATH) else ('a', False)
        with open(OUT_PATH, mode) as f:
            df.to_csv(f, header=header, index=False)
