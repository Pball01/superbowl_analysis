from redshift_schema import copy_data
from tw_raw_data import create_raw_folders, pull_twitter_data
from tw_process import preprocess_twitter
from stock import pull_stock_data, preprocess_stock
from gtrends import pull_google_trends
from redshift_ready import data_tables

"""
Define CONSTANTS
"""
# FILE_PATH=''
# S3_BUCKET=''
"""
if __name__ == "__main__":
      #LETS EXECUTE OUR PIPELINE (with first file: Date 2018-2-4)
  
  fact_table,dim_table=Pipeline(FILE_PATH)
  
  #save csv to S3
  fact_table.to_csv(S3_BUCKET+'fact_table.csv',index=False)
  dim_table.to_csv(S3_BUCKET+'dim_table.csv',index=False)

"""
# make sure to push company_dim to s3 bucket
# tw_raw_data.py file is using a smal number of companies to pull the data and test, use companies.pkl instead of
# companies_test.pkl to pull all companies data


def Pipeline():
    # pull_stock_data()  # pull stock data
    # preprocess_stock()  # preprocess stock

    # pull_google_trends()  # preprocess google trends

    # create_raw_folders()  # create new folder for Twitter
    # pull_twitter_data()  # pull twitter data
    # preprocess_twitter()  # preprocess twitter

    data_tables()  # creating tables for redshift
    copy_data()  # copy data from s3 to redshift


if __name__ == "__main__":
    Pipeline()
