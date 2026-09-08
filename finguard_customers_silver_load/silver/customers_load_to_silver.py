from pyspark import pipelines as dp #declarative pipeline
from pyspark.sql.dataframe import DataFrame as df
from pyspark.sql import functions as F



@dp.table(
    name = "finguard.silver.customers"
    ,comment = "Ingestion of customers data from bronze layer to silver layer"
)

@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_email","email IS NOT NULL")

def customers_silver() -> df:
    bronze_customers_df = spark.readStream.table("finguard.bronze.customers")
    
    transformed_customers_df = bronze_customers_df.select(
        F.col("customer_id")
        ,F.col("first_name")
        ,F.col("last_name")
        ,F.col("gender")
        ,F.col("age")
        ,F.col("city")
        ,F.col("state")
        ,F.col("country")
        ,F.col("annual_income")
        ,F.col("customer_segment")
        ,F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date")
        ,F.col("risk_score")
        ,F.col("preferred_spending_min")
        ,F.col("preferred_spending_max")
        ,F.col("preferred_city")
        ,F.col("preferred_country")
        ,F.col("trusted_device_id")
        ,F.col("card_number")
        ,F.col("card_type")
        ,F.col("email")
        ,F.col("transaction_limit")
        ,F.col("update_timestamp")
        ,F.current_timestamp().alias("silver_ingestion_time")
    )

    return transformed_customers_df


    





