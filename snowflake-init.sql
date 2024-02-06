-- Set up Snowflake db

use role accountadmin;
create database sales_db;

-- Set up schema, 
create schema sales_db.sales_schema;
use sales_db.sales_schema;

-- Set up table
create or replace table sales_db.sales_schema.sales_summary (
    trans_dt date,
    store_key integer,
    prod_key integer,
    sum_sales_qty integer,
    load_date date
);

-- Set up Stage
create or replace stage s3_stage_sales
url = 's3://<S3_BUCKET>/output/'
credentials =(
    aws_key_id = '<KEY_ID>' 
    aws_secret_key = '<SECRET_KEY>'
);
show stages;
list @s3_stage_sales;

-- Set up File Format
create or replace file format parquet_ff type = 'parquet';
