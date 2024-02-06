use database sales_db;
use schema sales_schema;

COPY INTO sales_summary FROM (
    SELECT 
        $1:trans_dt,
        $1:store_key,
        $1:prod_key,
        $1:sum_sales_qty,
        current_date
    FROM @s3_stage_sales
) pattern = '.*.gz.parquet' file_format = 'parquet_ff';