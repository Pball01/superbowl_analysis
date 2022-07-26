DROP SCHEMA IF EXISTS superbowl_test;

CREATE SCHEMA superbowl_test;

DROP TABLE IF EXISTS superbowl_test.tweet;
CREATE TABLE superbowl_test.tweet(
  tweet_id bigint,
  company_id int,
  tweet_url varchar,
  tweet_text varchar(2240),
  date_time datetime,
  retweet_count int,
  reply_count int, 
  like_count int,
  username varchar,
  display_name varchar,
  followers_count int,
  friends_count int,
  day int,
  month int,
  year int,
  directory varchar,
  contains_superbowl boolean
);

DROP TABLE IF EXISTS superbowl_test.company;
CREATE TABLE superbowl_test.company(
  company_id int,
  product_type varchar,
  company_name varchar,
  stock_ticker varchar
);

DROP TABLE IF EXISTS superbowl_test.google;
CREATE TABLE superbowl_test.google(
  trend_id int,
  date_time date,
  company_id int,
  value decimal
);

DROP TABLE IF EXISTS superbowl_test.date;
CREATE TABLE superbowl_test.date(
  date_id int,
  date date,
  month int,
  year int,
  superbowl_day boolean 
);

DROP TABLE IF EXISTS superbowl_test.stock;
CREATE TABLE superbowl_test.stock(
  stock_id int,
  date date,
  company_id int,
  value_type varchar,
  value float
);

DROP TABLE IF EXISTS superbowl_test.facts;
CREATE TABLE superbowl_test.facts(
  id int,
  tweet_id bigint,
  company_id int,
  compound float,
  compound_score varchar, 
  date_id int,
  trend_id int,
  stock_id int,
  superbowl_ad_id int
);

DROP TABLE IF EXISTS superbowl_test.superbowl_ad;
CREATE TABLE superbowl_test.superbowl_ad(
  superbowl_ad_id int,
  company_id int,
  year int,
  superbowl_ad boolean
)