DELETE FROM superbowl_test.tweet;

COPY superbowl_test.tweet
FROM 's3://superbowl-test/redshift/twitter_dim.csv'
REGION 'us-east-1'
IAM_ROLE 'arn:aws:iam::242171626770:role/RedshiftReadOnlyS3'
FORMAT CSV
IGNOREHEADER 1
EMPTYASNULL;

DELETE FROM superbowl_test.date;

COPY superbowl_test.date
FROM 's3://superbowl-test/redshift/date_dim.csv'
REGION 'us-east-1'
IAM_ROLE 'arn:aws:iam::242171626770:role/RedshiftReadOnlyS3'
FORMAT CSV
IGNOREHEADER 1
EMPTYASNULL;

DELETE FROM superbowl_test.superbowl_ad;

COPY superbowl_test.superbowl_ad
FROM 's3://superbowl-test/redshift/superbowlad_dim.csv'
REGION 'us-east-1'
IAM_ROLE 'arn:aws:iam::242171626770:role/RedshiftReadOnlyS3'
FORMAT CSV
IGNOREHEADER 1
EMPTYASNULL;