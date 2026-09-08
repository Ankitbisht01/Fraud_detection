from pyspark import pipelines as dp #declarative pipeline
from pyspark.sql.dataframe import DataFrame as df
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType



@dp.table(
    name = "finguard.silver.transactions"
    ,comment = "Ingestion of data from broze laye to silver layer"
)

@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect("valid_amount","amount > 1000")

def transactions_silver() -> df:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")
    
    schema = StructType([

        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True)
    ])

    
    tranformed_df = bronze_df.select(
        F.from_json(col("value"),schema).alias("data")
        ,F.col("topic").alias("kafka_topic")
        ,F.col("partition").alias("kafka_partition")
        ,F.col("offset").alias("kafka_offset")
        ,F.col("timestamp").alias("kafka_timestamp")
      , F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        F.col("data.transaction_id")
        ,F.col("data.customer_id")
        ,F.col("data.card_number")
        ,F.col("data.merchant_id")
        ,F.col("data.merchant_name")
        ,F.col("data.merchant_category")
        ,F.col("data.amount")
        ,F.col("data.currency")
        ,F.col("data.transaction_type")
        ,F.col("data.payment_channel")
        ,F.col("data.device_id")
        ,F.col("data.city")
        ,F.col("data.country")
        ,F.to_timestamp(F.col("data.transaction_timestamp")).alias("transaction_timestamp")
        ,F.col("data.is_international")
        ,F.col("data.status")
        ,F.col("kafka_topic")
        ,F.col("kafka_partition")
        ,F.col("kafka_offset")
        ,F.col("kafka_timestamp")
        ,F.col("bronze_ingestion_timestamp")
        ,F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return tranformed_df


    





