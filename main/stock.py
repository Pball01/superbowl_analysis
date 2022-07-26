import pandas_datareader
import numpy as np 
import pandas as pd 

from datetime import datetime 
import yfinance as yf 
 
# settings for importing Python and pandas libraries
import pandas_datareader.data as web
import datetime
import boto3
from my_aws_secrets import s3_access_key_id, s3_secret_access_key,s3_bucket

# create a client to access resources with
s3_client = boto3.client(
    's3', # add the service name we are accessing
    aws_access_key_id = s3_access_key_id, #update this
    aws_secret_access_key = s3_secret_access_key #update this
)

raw_file = "yahoo_finance/raw_stock.csv"
AWS_S3_BUCKET = "superbowl-test"
preprocessed_file  = "yahoo_finance/preprocessed_stock.csv"


#reading symbols
symbol = list(pd.read_csv('company_dim.csv')['stock_ticker'].dropna().unique())

#the start expression is for January 1, 2015
start = datetime.date(2015, 1, 1)

#the end expression is for March 31, 2022
end = datetime.date(2022, 3, 31)

#set path for csv file 
#path_out = 'superbowl_output/'
#file_out = 'superbowl_tickers_raw.csv'

def pull_stock_data():
    i=0 #for symbols in list file
    j=0 #for number of symbol retries
    while i<len(symbol):
        try:
            df = web.DataReader(symbol[i], 'yahoo', start, end)
            df.insert(0,'Symbol',symbol[i])
            df = df.drop(['Adj Close'], axis=1)
            if i == 0:
                #df.to_csv(path_out+file_out)
                df.to_csv(
                f"s3://{AWS_S3_BUCKET}/{raw_file}",
                #index=False,
                storage_options={
                    "key": s3_access_key_id,
                    "secret": s3_secret_access_key
                    },
                )

                print (i, symbol[i],'has data stored to csv file')
            else:

                #df.to_csv(path_out+file_out,mode = 'a',header=False)
                df.to_csv(
                f"s3://{AWS_S3_BUCKET}/{raw_file}",
                #index=False,
                mode = 'a', header = False,
                storage_options={
                    "key": s3_access_key_id,
                    "secret": s3_secret_access_key
                    },
                )                


                print (i, symbol[i],'has data stored to csv file')
        except:
            print ("Try:", j, "for symbol:", i)
            print("No information for symbol or file is open in Excel:")
            print (i,symbol[i])
            j=j+1
            if j == 15:
                j=0
                exit
            else: 
                continue
        i=i+1


def preprocess_stock():
    #read stored csv dataset as a dataframe
    #df = pd.read_csv('superbowl_output/superbowl_tickers_raw.csv')

    df = pd.read_csv(
        f"s3://{AWS_S3_BUCKET}/{raw_file}",
        storage_options={
            "key": s3_access_key_id,
            "secret": s3_secret_access_key
        },
    )


    #reset index and convert dataframe from wide format to long format using melt() function 
    df = df.reset_index()
    pd.melt(df, id_vars='Date', value_vars=['Symbol', 'High', 'Low', 'Open', 'Close', 'Volume'])


    #convert dataframe from wide format to long format using pd.melt() function 
    df_melted = pd.melt(df, id_vars=["Date", "Symbol"], value_vars=['High', 'Low', 'Open', 'Close', 'Volume'])
    df_melted = df_melted.rename(columns = {"Symbol": "Ticker Name",
                                        "variable": "Value Type",
                                        "value": "Values"})

    #df_melted.to_csv('superbowl_output/superbowl_tickers_data_preprocessed.csv')
    df_melted.to_csv(
    f"s3://{AWS_S3_BUCKET}/{preprocessed_file}",
    #index=False,
    #mode = 'a', header = False,
    storage_options={
    "key": s3_access_key_id,
    "secret": s3_secret_access_key
    },)     

    return df_melted

#if __name__ == "__main__":
#    pull_stock_data()
#    preprocess_stock()



