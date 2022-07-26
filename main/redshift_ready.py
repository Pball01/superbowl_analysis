#!/home/ec2-user/my_app/env/bin/python
import pandas as pd
import numpy as np
import s3_utils


twitter = s3_utils.get_pandas_df("twitter/preprocessed/twitter.csv")
trends = s3_utils.get_pandas_df("google_trends/preprocessed_trends.csv")
# stock = s3_utils.get_pandas_df("yahoo_finance/preprocessed/superbowl_stockdata_2015_2022.csv",
#                     usecols = ['stock_id', 'Date', 'Ticker Name', 'Value Type', 'Values'])
stock = s3_utils.get_pandas_df("yahoo_finance/preprocessed_stock.csv", index_col=[0])

company = s3_utils.get_pandas_df("files/company_dim.csv")
superbowl_ad = s3_utils.get_pandas_df("files/superbowl_ad.csv")

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


def twitter_ready(df):
    duplicated = list(
        df[df.duplicated(["Tweet Id"]) == True].sort_values("Tweet Id")["Tweet Id"]
    )
    df = df[df["Tweet Id"].isin(duplicated) == False].reset_index(drop=True)
    df["brand"] = df["brand"].str.lower()
    df["brand"] = df["brand"].replace(to_replace="wix", value="wix.com")
    df["brand"] = df["brand"].replace(to_replace="budlight", value="bud light")
    twitter_dim = df.merge(
        company, left_on="brand", right_on="company_name", how="inner"
    )
    twitter_dim = twitter_dim[
        [
            "Tweet Id",
            "company_id",
            "url",
            "Text",
            "Datetime",
            "Retweet Count",
            "Reply Count",
            "Like Count",
            "Username",
            "Display Name",
            "Followers Count",
            "Friends Count",
            "Day",
            "Month",
            "Year",
            "directory",
            "contains_superbowl",
        ]
    ]
    s3_utils.put_pandas_df("redshift/twitter_dim.csv", twitter_dim, index=False)
    print("Twitter Dimension Table - DONE!")
    return twitter_dim


def trends_ready(df):
    df.columns = df.columns.str.lower()
    df = df.melt(id_vars=["date"])
    df = df.rename(columns={"variable": "company_name"})
    df = df.reset_index().rename({"index": "trends_id"}, axis=1)
    trends_dim = df.merge(company, on="company_name", how="inner")
    trends_dim["date"] = pd.to_datetime(trends_dim["date"])
    trends_dim = trends_dim[["trends_id", "date", "company_id", "value"]]
    s3_utils.put_pandas_df("redshift/google_trends_dim.csv", trends_dim, index=False)
    print("Google Trends Dimension Table - DONE!")
    return trends_dim


def stock_ready(df):
    stock_dim = df.merge(
        company, left_on="Ticker Name", right_on="stock_ticker", how="inner"
    )
    stock_dim = stock_dim.reset_index().rename({"index": "stock_id"}, axis=1)
    stock_dim["Date"] = pd.to_datetime(stock_dim["Date"])
    stock_dim = stock_dim[["stock_id", "Date", "company_id", "Value Type", "Values"]]
    s3_utils.put_pandas_df("redshift/stock_dim.csv", stock_dim, index=False)
    print("Stock Dimension Table - DONE!")
    return stock_dim


def date_ready(start, end):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Month"] = df.Date.dt.month
    df["Year"] = df.Date.dt.year
    date_dim = df.copy()
    date_dim["date_id"] = date_dim.index
    date_dim["superbowl_day"] = date_dim["Date"].isin(superbowl_dates)
    date_dim = date_dim[["date_id", "Date", "Month", "Year", "superbowl_day"]]
    s3_utils.put_pandas_df("redshift/date_dim.csv", date_dim, index=False)
    print("Date Dimension Table - DONE!")
    return date_dim


def superbowlad_ready(df):
    df = pd.melt(df, id_vars="company")
    df = df.rename(columns={"variable": "year", "value": "superbowl_ad"})
    superbowlad_dim = company.merge(
        df, left_on="company_name", right_on="company", how="inner"
    )
    superbowlad_dim = superbowlad_dim.reset_index().rename(
        {"index": "superbowlad_id"}, axis=1
    )
    superbowlad_dim = superbowlad_dim[
        ["superbowlad_id", "company_id", "year", "superbowl_ad"]
    ]
    superbowlad_dim["year"] = superbowlad_dim["year"].astype(int)
    s3_utils.put_pandas_df("redshift/superbowlad_dim.csv", superbowlad_dim, index=False)
    print("Superbowl Ad Dimension Table - DONE!")
    return superbowlad_dim


def facts_ready(df, trends_dim, stock_dim, date_dim, superbowlad_dim):
    duplicated = list(
        df[df.duplicated(["Tweet Id"]) == True].sort_values("Tweet Id")["Tweet Id"]
    )
    df = df[df["Tweet Id"].isin(duplicated) == False].reset_index(drop=True)
    df["brand"] = df["brand"].str.lower()
    df["brand"] = df["brand"].replace(to_replace="wix", value="wix.com")
    df["brand"] = df["brand"].replace(to_replace="budlight", value="bud light")
    facts = df.merge(company, left_on="brand", right_on="company_name", how="inner")
    facts["Date"] = pd.to_datetime(facts["Datetime"])
    facts["Date"] = facts["Date"].dt.strftime("%Y-%m-%d")
    facts["Date"] = pd.to_datetime(facts["Date"])
    trends_dim["date"] = pd.to_datetime(trends_dim["date"])
    trends_dim["Month"] = trends_dim["date"].dt.month
    trends_dim["Year"] = trends_dim["date"].dt.year
    stock_dim["Date"] = pd.to_datetime(stock_dim["Date"])
    stock_dim["Month"] = stock_dim["Date"].dt.month
    stock_dim["Year"] = stock_dim["Date"].dt.year
    facts1 = facts[["Date", "Tweet Id", "compound", "comp_score", "company_id"]]
    # joining stock with trends data
    stock_google_trends = stock_dim.merge(
        trends_dim,
        left_on=["Date", "company_id"],
        right_on=["date", "company_id"],
        how="inner",
        suffixes=["_S", "_T"],
    )
    # joining date tables with stock table
    stock_google_trends_date = date_dim.merge(
        stock_google_trends, left_on=["Date"], right_on=["Date"], how="inner"
    )
    stock_google_trends_date_sp = stock_google_trends_date.merge(
        superbowlad_dim,
        left_on=["company_id", "Year"],
        right_on=["company_id", "year"],
        how="inner",
    )
    # combining the stock, trends, date table with twitter
    facts2 = stock_google_trends_date_sp.merge(
        facts1,
        left_on=["Date", "company_id"],
        right_on=["Date", "company_id"],
        how="inner",
    )
    facts3 = facts2[
        [
            "Tweet Id",
            "company_id",
            "compound",
            "comp_score",
            "date_id",
            "trends_id",
            "stock_id",
            "superbowlad_id",
        ]
    ]
    facts3 = facts3.reset_index().rename({"index": "id"}, axis=1)
    facts3["Tweet Id"] = pd.to_numeric(facts3["Tweet Id"], errors="coerce").astype(
        "Int64"
    )
    facts3["company_id"] = pd.to_numeric(facts3["company_id"], errors="coerce").astype(
        "Int64"
    )
    facts3["date_id"] = pd.to_numeric(facts3["date_id"], errors="coerce").astype(
        "Int64"
    )
    facts3["trends_id"] = pd.to_numeric(facts3["trends_id"], errors="coerce").astype(
        "Int64"
    )
    facts3["stock_id"] = pd.to_numeric(facts3["stock_id"], errors="coerce").astype(
        "Int64"
    )
    facts3["superbowlad_id"] = pd.to_numeric(
        facts3["superbowlad_id"], errors="coerce"
    ).astype("Int64")
    s3_utils.put_pandas_df("redshift/facts.csv", facts3, index=False)
    print("Facts Table - DONE!")
    return facts3


def data_tables():
    s3_utils.put_pandas_df("redshift/company_dim.csv", company, index=False)
    twitter_ready(twitter)
    trends_dim = trends_ready(trends)
    stock_dim = stock_ready(stock)
    date_dim = date_ready(start="2015-01-01", end="2022-03-31")
    superbowlad_dim = superbowlad_ready(superbowl_ad)
    facts_ready(twitter, trends_dim, stock_dim, date_dim, superbowlad_dim)
    print("All tables - DONE!")


if __name__ == "__main__":
    data_tables()
