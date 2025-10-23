# Databricks notebook source
"""
***************************************************************************************************
Program Name  : Hive_to_Databricks_PySpark_v3.py
Description   : Databricks PySpark ETL to compute Top 3 categories by revenue per region 
                (last 12 months) with stepwise processing, version control, and audit logging.
Original File : top3_category_revenue_per_region_with_audit.hql
Author        : Senior Data Engineer (Converted from Hive)
Created On    : 2025-01-23
Version       : 3

Error in previous version:
- Job execution failed with "Workload failed, see run output for details"
- Multiple COMMAND cells might be causing execution issues
- Code structure not compatible with job execution

Error handling:
- Consolidated all code into single executable block
- Removed COMMAND cell separators for job compatibility
- Simplified execution flow
- Added comprehensive error handling and logging
***************************************************************************************************
"""

# Import all required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, current_timestamp, current_date, lit, rank, add_months
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from datetime import datetime, timedelta
import traceback
import uuid as uuid_lib
import random

# Initialize Spark Session
spark = SparkSession.builder.appName("TOP3_CATEGORY_REVENUE_PER_REGION").getOrCreate()
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Define runtime variables
RUN_ID = str(uuid_lib.uuid4())
LOAD_TS = datetime.now()
PROCESS_NAME = 'TOP3_CATEGORY_REVENUE_PER_REGION'

print("="*80)
print(f"Process Name: {PROCESS_NAME}")
print(f"Run ID: {RUN_ID}")
print(f"Load Timestamp: {LOAD_TS}")
print("="*80)

# Utility function to check table existence
def table_exists(table_name):
    try:
        spark.table(table_name)
        return True
    except:
        return False

# Audit logging function
def log_process_status(process_name, run_id, status, message):
    try:
        log_df = spark.createDataFrame([
            (process_name, run_id, status, datetime.now(), message)
        ], ["process_name", "run_id", "status", "log_ts", "message"])
        log_df.write.format("delta").mode("append").saveAsTable("process_control_log")
        print(f"[{status}] {message}")
    except Exception as e:
        print(f"Error logging: {str(e)}")

# Create sample data function
def create_sample_data():
    print("\n=== Creating sample data tables ===")
    
    # Create regions table
    if not table_exists("regions"):
        print("Creating regions table...")
        regions_data = [(1, "North America"), (2, "Europe"), (3, "Asia Pacific"), (4, "Latin America"), (5, "Middle East")]
        regions_df = spark.createDataFrame(regions_data, ["region_id", "region_name"])
        regions_df.write.format("delta").mode("overwrite").saveAsTable("regions")
        print("✓ Regions table created")
    
    # Create products table
    if not table_exists("products"):
        print("Creating products table...")
        products_data = []
        categories = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for i in range(1, 101):
            products_data.append((i, f"Product_{i}", random.choice(categories)))
        products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category_id"])
        products_df.write.format("delta").mode("overwrite").saveAsTable("products")
        print("✓ Products table created")
    
    # Create sales table
    if not table_exists("sales"):
        print("Creating sales table...")
        sales_data = []
        base_date = datetime.now().date()
        for i in range(1, 1001):
            order_date = base_date - timedelta(days=random.randint(0, 365))
            sales_data.append((i, random.randint(1, 100), random.randint(1, 5), order_date, random.randint(1, 10), round(random.uniform(10.0, 500.0), 2)))
        sales_df = spark.createDataFrame(sales_data, ["order_id", "product_id", "region_id", "order_date", "quantity", "price"])
        sales_df.write.format("delta").mode("overwrite").saveAsTable("sales")
        print("✓ Sales table created")
    
    # Create process_control_log table
    if not table_exists("process_control_log"):
        print("Creating process_control_log table...")
        schema = StructType([StructField("process_name", StringType(), True), StructField("run_id", StringType(), True), StructField("status", StringType(), True), StructField("log_ts", TimestampType(), True), StructField("message", StringType(), True)])
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable("process_control_log")
        print("✓ Process control log table created")
    
    # Create output table
    if not table_exists("top3_category_revenue_region_versions"):
        print("Creating top3_category_revenue_region_versions table...")
        schema = StructType([StructField("run_id", StringType(), True), StructField("region_id", IntegerType(), True), StructField("category_id", IntegerType(), True), StructField("total_revenue", DecimalType(10, 2), True), StructField("avg_order_value", DecimalType(10, 2), True), StructField("category_rank", IntegerType(), True), StructField("load_ts", TimestampType(), True), StructField("source_system", StringType(), True), StructField("update_date", TimestampType(), True)])
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable("top3_category_revenue_region_versions")
        print("✓ Output table created")
    
    print("Sample data creation completed\n")

# Create sample data
create_sample_data()

# Log process start
log_process_status(PROCESS_NAME, RUN_ID, "STARTED", "Process initiated")

try:
    # STEP 1: Filter sales for last 12 months
    print("\n=== STEP 1: Filtering sales for last 12 months ===")
    sales_df = spark.table("sales")
    products_df = spark.table("products")
    regions_df = spark.table("regions")
    twelve_months_ago = add_months(current_date(), -12)
    
    filtered_sales_df = sales_df.filter(col("order_date") >= twelve_months_ago).join(products_df, sales_df.product_id == products_df.product_id, "inner").join(regions_df, sales_df.region_id == regions_df.region_id, "inner").select(sales_df.order_id, sales_df.product_id, products_df.category_id, regions_df.region_id, sales_df.order_date, (col("quantity") * col("price")).cast("decimal(10,2)").alias("revenue"))
    
    filtered_sales_df.cache()
    row_count_step1 = filtered_sales_df.count()
    print(f"Filtered sales records: {row_count_step1}")
    filtered_sales_df.write.format("delta").mode("overwrite").saveAsTable("tmp_filtered_sales")
    
    # STEP 2: Calculate aggregates
    print("\n=== STEP 2: Calculating revenue aggregates ===")
    category_revenue_df = filtered_sales_df.groupBy("region_id", "category_id").agg(_sum("revenue").alias("total_revenue"), _avg("revenue").alias("avg_order_value"))
    row_count_step2 = category_revenue_df.count()
    print(f"Category-Region combinations: {row_count_step2}")
    category_revenue_df.write.format("delta").mode("overwrite").saveAsTable("tmp_category_revenue")
    
    # STEP 3: Rank categories
    print("\n=== STEP 3: Ranking categories by revenue ===")
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    ranked_categories_df = category_revenue_df.withColumn("category_rank", rank().over(window_spec))
    row_count_step3 = ranked_categories_df.count()
    print(f"Ranked categories: {row_count_step3}")
    ranked_categories_df.write.format("delta").mode("overwrite").saveAsTable("tmp_ranked_categories")
    
    # STEP 4: Insert top 3
    print("\n=== STEP 4: Inserting top 3 categories per region ===")
    top3_df = ranked_categories_df.filter(col("category_rank") <= 3).withColumn("run_id", lit(RUN_ID)).withColumn("load_ts", lit(LOAD_TS)).withColumn("source_system", lit("HIVE_MIGRATION")).withColumn("update_date", current_timestamp()).select("run_id", "region_id", "category_id", "total_revenue", "avg_order_value", "category_rank", "load_ts", "source_system", "update_date").orderBy("region_id", "category_rank")
    
    final_row_count = top3_df.count()
    print(f"Top 3 records to insert: {final_row_count}")
    print("\nSample results:")
    top3_df.show(15, truncate=False)
    top3_df.write.format("delta").mode("append").saveAsTable("top3_category_revenue_region_versions")
    
    # Cleanup
    print("\n=== Cleaning up temporary tables ===")
    spark.sql("DROP TABLE IF EXISTS tmp_filtered_sales")
    spark.sql("DROP TABLE IF EXISTS tmp_category_revenue")
    spark.sql("DROP TABLE IF EXISTS tmp_ranked_categories")
    filtered_sales_df.unpersist()
    
    # Log success
    success_message = f"Data successfully inserted into versioned results table. Total records: {final_row_count}"
    log_process_status(PROCESS_NAME, RUN_ID, "SUCCESS", success_message)
    
    print("\n" + "="*80)
    print("PROCESS COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Run ID: {RUN_ID}")
    print(f"Records Processed: {final_row_count}")
    print(f"Completion Time: {datetime.now()}")
    print("="*80)
    
except Exception as e:
    error_message = f"Process failed with error: {str(e)}\n{traceback.format_exc()}"
    log_process_status(PROCESS_NAME, RUN_ID, "FAILED", error_message)
    print("\n" + "="*80)
    print("PROCESS FAILED")
    print("="*80)
    print(error_message)
    print("="*80)
    raise e
