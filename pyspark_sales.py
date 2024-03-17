# Import SparkSession
import sys
from pyspark.sql import SparkSession

date_str = sys.argv[1]
file_input = sys.argv[2]
s3_bucket = sys.argv[3]

# Create SparkSession
spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()

sales_df = (
    spark.read.option("header", "true").option("delimiter", ",").csv(f"{file_input}")
)

sales_df.createOrReplaceTempView("sales")

df_sum_sales_qty = spark.sql(
    "select trans_dt,store_key,prod_key,sum(sales_qty) as sum_sales_qty from sales group by 1,2,3;"
)

df_sum_sales_qty.show(2)

(
    df_sum_sales_qty.repartition(1)
    .write.mode("overwrite")
    .option("compression", "gzip")
    .parquet(f"s3://{s3_bucket}/output/sales_date={date_str}")
)
