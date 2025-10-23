# Databricks notebook source
"""
***************************************************************************************************
Program Name  : Hive_to_Databricks_PySpark_v1.py
Description   : Databricks PySpark ETL to compute Top 3 categories by revenue per region 
                (last 12 months) with stepwise processing, version control, and audit logging.
Original Hive : top3_category_revenue_per_region_with_audit.hql
Author        : Senior Data Engineer (Converted from Hive)
Created On    : 2025-01-23
Version       : 1
***************************************************************************************************
"""

# ===============================================================================================
# IMPORT REQUIRED LIBRARIES
# ===============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, sum as _sum, avg as _avg,
    rank, months_between, add_months, expr, uuid
)
from pyspark.sql.window import Window
from datetime import datetime
import traceback

# ===============================================================================================
# INITIALIZE SPARK SESSION
# ===============================================================================================
spark = SparkSession.builder \
    .appName("TOP3_CATEGORY_REVENUE_PER_REGION") \
    .getOrCreate()

# Enable Delta schema evolution if needed
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ===============================================================================================
# DEFINE RUNTIME VARIABLES
# ===============================================================================================
run_id = str(uuid.uuid4())
load_ts = datetime.now()
process_name = "TOP3_CATEGORY_REVENUE_PER_REGION"
user_identity = spark.sparkContext.sparkUser()

print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"Load Timestamp: {load_ts}")
print(f"User: {user_identity}")

# ===============================================================================================
# AUDIT LOGGING FUNCTION
# ===============================================================================================
def log_to_control_table(process_name, run_id, status, message):
    """
    Logs process execution status to the control table.
    
    Args:
        process_name (str): Name of the process
        run_id (str): Unique run identifier
        status (str): Status (STARTED, SUCCESS, FAILED)
        message (str): Log message
    """
    try:
        log_df = spark.createDataFrame([
            (process_name, run_id, status, datetime.now(), message)
        ], ["process_name", "run_id", "status", "log_ts", "message"])
        
        log_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("process_control_log")
        
        print(f"[{status}] {message}")
    except Exception as e:
        print(f"Error logging to control table: {str(e)}")

# ===============================================================================================
# LOG PROCESS START
# ===============================================================================================
log_to_control_table(process_name, run_id, "STARTED", "Process initiated")

try:
    start_time = datetime.now()
    
    # ===============================================================================================
    # STEP 1: FILTER SALES FOR LAST 12 MONTHS
    # ===============================================================================================
    print("\n[STEP 1] Filtering sales for last 12 months...")
    
    # Read source tables
    sales_df = spark.table("sales")
    products_df = spark.table("products")
    regions_df = spark.table("regions")
    
    # Calculate date threshold (12 months ago)
    twelve_months_ago = add_months(current_date(), -12)
    
    # Filter and join data
    filtered_sales_df = sales_df \
        .filter(col("order_date") >= twelve_months_ago) \
        .join(products_df, sales_df.product_id == products_df.product_id, "inner") \
        .join(regions_df, sales_df.region_id == regions_df.region_id, "inner") \
        .select(
            sales_df.order_id,
            sales_df.product_id,
            products_df.category_id,
            regions_df.region_id,
            sales_df.order_date,
            (col("quantity") * col("price")).cast("decimal(10,2)").alias("revenue")
        )
    
    # Cache for reuse
    filtered_sales_df.cache()
    filtered_sales_count = filtered_sales_df.count()
    print(f"Filtered sales records: {filtered_sales_count}")
    
    # ===============================================================================================
    # STEP 2: CALCULATE TOTAL AND AVERAGE REVENUE PER CATEGORY PER REGION
    # ===============================================================================================
    print("\n[STEP 2] Calculating revenue aggregates per category per region...")
    
    category_revenue_df = filtered_sales_df \
        .groupBy("region_id", "category_id") \
        .agg(
            _sum("revenue").alias("total_revenue"),
            _avg("revenue").alias("avg_order_value")
        )
    
    category_revenue_count = category_revenue_df.count()
    print(f"Category-Region combinations: {category_revenue_count}")
    
    # ===============================================================================================
    # STEP 3: RANK CATEGORIES BY REVENUE WITHIN EACH REGION
    # ===============================================================================================
    print("\n[STEP 3] Ranking categories by revenue within each region...")
    
    # Define window specification
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    # ===============================================================================================
    # STEP 4: FILTER TOP 3 PER REGION
    # ===============================================================================================
    print("\n[STEP 4] Filtering top 3 categories per region...")
    
    top3_df = ranked_categories_df \
        .filter(col("category_rank") <= 3) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("load_ts", lit(load_ts)) \
        .withColumn("source_system", lit("HIVE_MIGRATION")) \
        .withColumn("update_date", current_timestamp()) \
        .select(
            "run_id",
            "region_id",
            "category_id",
            "total_revenue",
            "avg_order_value",
            "category_rank",
            "load_ts",
            "source_system",
            "update_date"
        ) \
        .orderBy("region_id", "category_rank")
    
    top3_count = top3_df.count()
    print(f"Top 3 records to insert: {top3_count}")
    
    # ===============================================================================================
    # STEP 5: WRITE TO VERSIONED DELTA TABLE
    # ===============================================================================================
    print("\n[STEP 5] Writing results to Delta table...")
    
    top3_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("top3_category_revenue_region_versions")
    
    print("Data successfully written to top3_category_revenue_region_versions")
    
    # ===============================================================================================
    # CALCULATE EXECUTION METRICS
    # ===============================================================================================
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    print(f"\n{'='*80}")
    print("EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"Process Name       : {process_name}")
    print(f"Run ID             : {run_id}")
    print(f"User               : {user_identity}")
    print(f"Start Time         : {start_time}")
    print(f"End Time           : {end_time}")
    print(f"Execution Time     : {execution_time:.2f} seconds")
    print(f"Filtered Records   : {filtered_sales_count}")
    print(f"Category-Regions   : {category_revenue_count}")
    print(f"Top 3 Records      : {top3_count}")
    print(f"Status             : SUCCESS")
    print(f"{'='*80}")
    
    # ===============================================================================================
    # LOG SUCCESS
    # ===============================================================================================
    success_message = f"Data successfully inserted. Records: {top3_count}, Execution time: {execution_time:.2f}s"
    log_to_control_table(process_name, run_id, "SUCCESS", success_message)
    
    # Unpersist cached data
    filtered_sales_df.unpersist()
    
except Exception as e:
    # ===============================================================================================
    # ERROR HANDLING
    # ===============================================================================================
    error_message = f"Process failed with error: {str(e)}\n{traceback.format_exc()}"
    print(f"\n[ERROR] {error_message}")
    log_to_control_table(process_name, run_id, "FAILED", error_message)
    raise e

print("\n[COMPLETED] Process execution finished successfully.")
