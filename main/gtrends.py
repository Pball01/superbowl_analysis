import pytrends
from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os
import numpy
import boto3
from my_aws_secrets import s3_access_key_id, s3_secret_access_key,s3_bucket


# create a client to access resources with
s3_client = boto3.client(
    's3', # add the service name we are accessing
    aws_access_key_id = s3_access_key_id, #update this
    aws_secret_access_key = s3_secret_access_key #update this
)


AWS_S3_BUCKET = "superbowl-test"
preprocessed_file  = "google_trends/preprocessed_trends.csv"



beer = ['budweiser', 'bud light']
candy = ['mars']
cars = ['jeep', 'toyota']
food = ['doritos', 'avocado from mexico', 'pringles']
soft_drinks = ['pepsi', 'mountain dew', 'coca-cola']
phone = ['t-mobile', 'sprint']
other = ['tide', 'weather tech']
website = ['turbotax', 'wix.com', 'squarespace']
tech_delivery = ['amazon alexa', 'uber eats']


#2015 
pytrend1 = TrendReq()
pytrend2 = TrendReq()
pytrend3 = TrendReq()
pytrend4 = TrendReq()
pytrend5 = TrendReq()
pytrend6 = TrendReq()
pytrend7 = TrendReq()
pytrend8 = TrendReq()
pytrend9 = TrendReq()



#2016
pytrend10 = TrendReq()
pytrend11 = TrendReq()
pytrend12 = TrendReq()
pytrend13 = TrendReq()
pytrend14 = TrendReq()
pytrend15 = TrendReq()
pytrend16 = TrendReq()
pytrend17 = TrendReq()
pytrend18 = TrendReq()

#2017
pytrend19 = TrendReq()
pytrend20 = TrendReq()
pytrend21 = TrendReq()
pytrend22 = TrendReq()
pytrend23 = TrendReq()
pytrend24 = TrendReq()
pytrend25 = TrendReq()
pytrend26 = TrendReq()
pytrend27 = TrendReq()

#2018
pytrend28 = TrendReq()
pytrend29 = TrendReq()
pytrend30 = TrendReq()
pytrend31 = TrendReq()
pytrend32 = TrendReq()
pytrend33 = TrendReq()
pytrend34 = TrendReq()
pytrend35 = TrendReq()
pytrend36 = TrendReq()

#2019
pytrend37 = TrendReq()
pytrend38 = TrendReq()
pytrend39 = TrendReq()
pytrend40 = TrendReq()
pytrend41 = TrendReq()
pytrend42 = TrendReq()
pytrend43 = TrendReq()
pytrend44 = TrendReq()
pytrend45 = TrendReq()

#2020
pytrend46 = TrendReq()
pytrend47 = TrendReq()
pytrend48 = TrendReq()
pytrend49 = TrendReq()
pytrend50 = TrendReq()
pytrend51 = TrendReq()
pytrend52 = TrendReq()
pytrend53 = TrendReq()
pytrend54 = TrendReq()

#2021
pytrend55 = TrendReq()
pytrend56 = TrendReq()
pytrend57 = TrendReq()
pytrend58 = TrendReq()
pytrend59 = TrendReq()
pytrend60 = TrendReq()
pytrend61 = TrendReq()
pytrend62 = TrendReq()
pytrend63 = TrendReq()

#2022
pytrend64 = TrendReq()
pytrend65 = TrendReq()
pytrend66 = TrendReq()
pytrend67 = TrendReq()
pytrend68 = TrendReq()
pytrend69 = TrendReq()
pytrend70 = TrendReq()
pytrend71 = TrendReq()
pytrend72 = TrendReq()




pytrend1.build_payload(beer, timeframe='2015-01-30 2015-03-31')
pytrend2.build_payload(candy, timeframe='2015-01-30 2015-03-31')
pytrend3.build_payload(cars, timeframe='2015-01-30 2015-03-31')
pytrend4.build_payload(food, timeframe='2015-01-30 2015-03-31')
pytrend5.build_payload(soft_drinks, timeframe='2015-01-30 2015-03-31')
pytrend6.build_payload(phone, timeframe='2015-01-30 2015-03-31')
pytrend7.build_payload(other, timeframe='2015-01-30 2015-03-31')
pytrend8.build_payload(website, timeframe='2015-01-30 2015-03-31')
pytrend9.build_payload(tech_delivery, timeframe='2015-01-30 2015-03-31')

pytrend10.build_payload(beer, timeframe='2016-01-30 2016-03-31')
pytrend11.build_payload(candy, timeframe='2016-01-30 2016-03-31')
pytrend12.build_payload(cars, timeframe='2016-01-30 2016-03-31')
pytrend13.build_payload(food, timeframe='2016-01-30 2016-03-31')
pytrend14.build_payload(soft_drinks, timeframe='2016-01-30 2016-03-31')
pytrend15.build_payload(phone, timeframe='2016-01-30 2016-03-31')
pytrend16.build_payload(other, timeframe='2016-01-30 2016-03-31')
pytrend17.build_payload(website, timeframe='2016-01-30 2016-03-31')
pytrend18.build_payload(tech_delivery, timeframe='2016-01-30 2016-03-31')


pytrend19.build_payload(beer, timeframe='2017-01-30 2017-03-31')
pytrend20.build_payload(candy, timeframe='2017-01-30 2017-03-31')
pytrend21.build_payload(cars, timeframe='2017-01-30 2017-03-31')
pytrend22.build_payload(food, timeframe='2017-01-30 2017-03-31')
pytrend23.build_payload(soft_drinks, timeframe='2017-01-30 2017-03-31')
pytrend24.build_payload(phone, timeframe='2017-01-30 2017-03-31')
pytrend25.build_payload(other, timeframe='2017-01-30 2017-03-31')
pytrend26.build_payload(website, timeframe='2017-01-30 2017-03-31')
pytrend27.build_payload(tech_delivery, timeframe='2017-01-30 2017-03-31')


pytrend28.build_payload(beer, timeframe='2018-01-30 2018-03-31')
pytrend29.build_payload(candy, timeframe='2018-01-30 2018-03-31')
pytrend30.build_payload(cars, timeframe='2018-01-30 2018-03-31')
pytrend31.build_payload(food, timeframe='2018-01-30 2018-03-31')
pytrend32.build_payload(soft_drinks, timeframe='2018-01-30 2018-03-31')
pytrend33.build_payload(phone, timeframe='2018-01-30 2018-03-31')
pytrend34.build_payload(other, timeframe='2018-01-30 2018-03-31')
pytrend35.build_payload(website, timeframe='2018-01-30 2018-03-31')
pytrend36.build_payload(tech_delivery, timeframe='2018-01-30 2018-03-31')


pytrend37.build_payload(beer, timeframe='2019-01-30 2019-03-31')
pytrend38.build_payload(candy, timeframe='2019-01-30 2019-03-31')
pytrend39.build_payload(cars, timeframe='2019-01-30 2019-03-31')
pytrend40.build_payload(food, timeframe='2019-01-30 2019-03-31')
pytrend41.build_payload(soft_drinks, timeframe='2019-01-30 2019-03-31')
pytrend42.build_payload(phone, timeframe='2019-01-30 2019-03-31')
pytrend43.build_payload(other, timeframe='2019-01-30 2019-03-31')
pytrend44.build_payload(website, timeframe='2019-01-30 2019-03-31')
pytrend45.build_payload(tech_delivery, timeframe='2019-01-30 2019-03-31')

pytrend46.build_payload(beer, timeframe='2020-01-30 2020-03-31')
pytrend47.build_payload(candy, timeframe='2020-01-30 2020-03-31')
pytrend48.build_payload(cars, timeframe='2020-01-30 2020-03-31')
pytrend49.build_payload(food, timeframe='2020-01-30 2020-03-31')
pytrend50.build_payload(soft_drinks, timeframe='2020-01-30 2020-03-31')
pytrend51.build_payload(phone, timeframe='2020-01-30 2020-03-31')
pytrend52.build_payload(other, timeframe='2020-01-30 2020-03-31')
pytrend53.build_payload(website, timeframe='2020-01-30 2020-03-31')
pytrend54.build_payload(tech_delivery, timeframe='2020-01-30 2020-03-31')

pytrend55.build_payload(beer, timeframe='2021-01-30 2021-03-31')
pytrend56.build_payload(candy, timeframe='2021-01-30 2021-03-31')
pytrend57.build_payload(cars, timeframe='2021-01-30 2021-03-31')
pytrend58.build_payload(food, timeframe='2021-01-30 2021-03-31')
pytrend59.build_payload(soft_drinks, timeframe='2021-01-30 2021-03-31')
pytrend60.build_payload(phone, timeframe='2021-01-30 2021-03-31')
pytrend61.build_payload(other, timeframe='2021-01-30 2021-03-31')
pytrend62.build_payload(website, timeframe='2021-01-30 2021-03-31')
pytrend63.build_payload(tech_delivery, timeframe='2021-01-30 2021-03-31')

pytrend64.build_payload(beer, timeframe='2022-01-30 2022-03-31')
pytrend65.build_payload(candy, timeframe='2022-01-30 2022-03-31')
pytrend66.build_payload(cars, timeframe='2022-01-30 2022-03-31')
pytrend67.build_payload(food, timeframe='2022-01-30 2022-03-31')
pytrend68.build_payload(soft_drinks, timeframe='2022-01-30 2022-03-31')
pytrend69.build_payload(phone, timeframe='2022-01-30 2022-03-31')
pytrend70.build_payload(other, timeframe='2022-01-30 2022-03-31')
pytrend71.build_payload(website, timeframe='2022-01-30 2022-03-31')
pytrend72.build_payload(tech_delivery, timeframe='2022-01-30 2022-03-31')



def pull_google_trends():
    beer_2015 = pytrend1.interest_over_time().drop(columns='isPartial')
    candy_2015 = pytrend2.interest_over_time().drop(columns='isPartial')
    cars_2015 = pytrend3.interest_over_time().drop(columns='isPartial')
    food_2015 = pytrend4.interest_over_time().drop(columns='isPartial')
    soft_drink_2015 = pytrend5.interest_over_time().drop(columns='isPartial')
    phone_2015 = pytrend6.interest_over_time().drop(columns='isPartial')
    other_2015 = pytrend7.interest_over_time().drop(columns='isPartial')
    website_2015 = pytrend8.interest_over_time().drop(columns='isPartial')
    tech_delivery_2015 = pytrend9.interest_over_time().drop(columns='isPartial')

    beer_2016 = pytrend10.interest_over_time().drop(columns='isPartial')
    candy_2016 = pytrend11.interest_over_time().drop(columns='isPartial')
    cars_2016 = pytrend12.interest_over_time().drop(columns='isPartial')
    food_2016 = pytrend13.interest_over_time().drop(columns='isPartial')
    soft_drink_2016 = pytrend14.interest_over_time().drop(columns='isPartial')
    phone_2016 = pytrend15.interest_over_time().drop(columns='isPartial')
    other_2016 = pytrend16.interest_over_time().drop(columns='isPartial')
    website_2016 = pytrend17.interest_over_time().drop(columns='isPartial')
    tech_delivery_2016 = pytrend18.interest_over_time().drop(columns='isPartial')

    beer_2017 = pytrend19.interest_over_time().drop(columns='isPartial')
    candy_2017 = pytrend20.interest_over_time().drop(columns='isPartial')
    cars_2017 = pytrend21.interest_over_time().drop(columns='isPartial')
    food_2017 = pytrend22.interest_over_time().drop(columns='isPartial')
    soft_drink_2017 = pytrend23.interest_over_time().drop(columns='isPartial')
    phone_2017 = pytrend24.interest_over_time().drop(columns='isPartial')
    other_2017 = pytrend25.interest_over_time().drop(columns='isPartial')
    website_2017 = pytrend26.interest_over_time().drop(columns='isPartial')
    tech_delivery_2017 = pytrend27.interest_over_time().drop(columns='isPartial')

    beer_2018 = pytrend28.interest_over_time().drop(columns='isPartial')
    candy_2018 = pytrend29.interest_over_time().drop(columns='isPartial')
    cars_2018 = pytrend30.interest_over_time().drop(columns='isPartial')
    food_2018 = pytrend31.interest_over_time().drop(columns='isPartial')
    soft_drink_2018 = pytrend32.interest_over_time().drop(columns='isPartial')
    phone_2018 = pytrend33.interest_over_time().drop(columns='isPartial')
    other_2018 = pytrend34.interest_over_time().drop(columns='isPartial')
    website_2018 = pytrend35.interest_over_time().drop(columns='isPartial')
    tech_delivery_2018 = pytrend36.interest_over_time().drop(columns='isPartial')

    beer_2019 = pytrend37.interest_over_time().drop(columns='isPartial')
    candy_2019 = pytrend38.interest_over_time().drop(columns='isPartial')
    cars_2019 = pytrend39.interest_over_time().drop(columns='isPartial')
    food_2019 = pytrend40.interest_over_time().drop(columns='isPartial')
    soft_drink_2019 = pytrend41.interest_over_time().drop(columns='isPartial')
    phone_2019 = pytrend42.interest_over_time().drop(columns='isPartial')
    other_2019 = pytrend43.interest_over_time().drop(columns='isPartial')
    website_2019 = pytrend44.interest_over_time().drop(columns='isPartial')
    tech_delivery_2019 = pytrend45.interest_over_time().drop(columns='isPartial')

    beer_2020 = pytrend46.interest_over_time().drop(columns='isPartial')
    candy_2020 = pytrend47.interest_over_time().drop(columns='isPartial')
    cars_2020 = pytrend48.interest_over_time().drop(columns='isPartial')
    food_2020 = pytrend49.interest_over_time().drop(columns='isPartial')
    soft_drink_2020 = pytrend50.interest_over_time().drop(columns='isPartial')
    phone_2020 = pytrend51.interest_over_time().drop(columns='isPartial')
    other_2020 = pytrend52.interest_over_time().drop(columns='isPartial')
    website_2020 = pytrend53.interest_over_time().drop(columns='isPartial')
    tech_delivery_2020 = pytrend54.interest_over_time().drop(columns='isPartial')

    beer_2021 = pytrend55.interest_over_time().drop(columns='isPartial')
    candy_2021 = pytrend56.interest_over_time().drop(columns='isPartial')
    cars_2021 = pytrend57.interest_over_time().drop(columns='isPartial')
    food_2021 = pytrend58.interest_over_time().drop(columns='isPartial')
    soft_drink_2021 = pytrend59.interest_over_time().drop(columns='isPartial')
    phone_2021 = pytrend60.interest_over_time().drop(columns='isPartial')
    other_2021 = pytrend61.interest_over_time().drop(columns='isPartial')
    website_2021 = pytrend62.interest_over_time().drop(columns='isPartial')
    tech_delivery_2021 = pytrend63.interest_over_time().drop(columns='isPartial')

    beer_2022 = pytrend64.interest_over_time().drop(columns='isPartial')
    candy_2022 = pytrend65.interest_over_time().drop(columns='isPartial')
    cars_2022 = pytrend66.interest_over_time().drop(columns='isPartial')
    food_2022 = pytrend67.interest_over_time().drop(columns='isPartial')
    soft_drink_2022 = pytrend68.interest_over_time().drop(columns='isPartial')
    phone_2022 = pytrend69.interest_over_time().drop(columns='isPartial')
    other_2022 = pytrend70.interest_over_time().drop(columns='isPartial')
    website_2022 = pytrend71.interest_over_time().drop(columns='isPartial')
    tech_delivery_2022 = pytrend72.interest_over_time().drop(columns='isPartial')

    All_2015 = beer_2015.merge(candy_2015,on='date').merge(cars_2015,on='date').merge(food_2015,on='date').merge(soft_drink_2015,on='date').merge(phone_2015,on='date').merge(other_2015,on='date').merge(website_2015,on='date').merge(tech_delivery_2015,on='date')
    All_2016 = beer_2016.merge(candy_2016,on='date').merge(cars_2016,on='date').merge(food_2016,on='date').merge(soft_drink_2016,on='date').merge(phone_2016,on='date').merge(other_2016,on='date').merge(website_2016,on='date').merge(tech_delivery_2016,on='date')
    All_2017 = beer_2017.merge(candy_2017,on='date').merge(cars_2017,on='date').merge(food_2017,on='date').merge(soft_drink_2017,on='date').merge(phone_2017,on='date').merge(other_2017,on='date').merge(website_2017,on='date').merge(tech_delivery_2017,on='date')
    All_2018 = beer_2018.merge(candy_2018,on='date').merge(cars_2018,on='date').merge(food_2018,on='date').merge(soft_drink_2018,on='date').merge(phone_2018,on='date').merge(other_2018,on='date').merge(website_2018,on='date').merge(tech_delivery_2018,on='date')
    All_2019 = beer_2019.merge(candy_2019,on='date').merge(cars_2019,on='date').merge(food_2019,on='date').merge(soft_drink_2019,on='date').merge(phone_2019,on='date').merge(other_2019,on='date').merge(website_2019,on='date').merge(tech_delivery_2019,on='date')
    All_2020 = beer_2020.merge(candy_2020,on='date').merge(cars_2020,on='date').merge(food_2020,on='date').merge(soft_drink_2020,on='date').merge(phone_2020,on='date').merge(other_2020,on='date').merge(website_2020,on='date').merge(tech_delivery_2020,on='date')
    All_2021 = beer_2021.merge(candy_2021,on='date').merge(cars_2021,on='date').merge(food_2021,on='date').merge(soft_drink_2021,on='date').merge(phone_2021,on='date').merge(other_2021,on='date').merge(website_2021,on='date').merge(tech_delivery_2021,on='date')
    All_2022 = beer_2022.merge(candy_2022,on='date').merge(cars_2022,on='date').merge(food_2022,on='date').merge(soft_drink_2022,on='date').merge(phone_2022,on='date').merge(other_2022,on='date').merge(website_2022,on='date').merge(tech_delivery_2022,on='date')

    df_final = pd.concat([All_2015, All_2016, All_2017, All_2018, All_2019, All_2020, All_2021, All_2022])
    
    #df_final.to_csv("superbowl_output/google_trends.csv")
    df_final.to_csv(
        f"s3://{AWS_S3_BUCKET}/{preprocessed_file}",
        storage_options={
        "key": s3_access_key_id,
        "secret": s3_secret_access_key
        },) 

    return df_final

#if __name__ == '__main__':
#    pull_google_trends()
