# Databricks notebook source
"""
***************************************************************************************************
Program Name  : Hive_to_Databricks_PySpark_v1.py
Description   : Databricks PySpark ETL to compute Top 3 categories by revenue per region 
                (last 12 months) with stepwise processing, version control, and audit logging.
Original File : top3_category_revenue_per_region_with_audit.hql
Author        : Senior Data Engineer (Converted from Hive)
Created On    : 2025-01-XX
Version       : 1
***************************************************************************************************
"""

# ===============================================================================================
# IMPORT REQUIRED LIBRARIES
# ===============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg as _avg, current_timestamp, lit, 
    add_months, current_date, rank, uuid, cast
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

# Enable schema evolution for Delta tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ===============================================================================================
# DEFINE RUNTIME VARIABLES
# ===============================================================================================
run_id = str(uuid.uuid4())
load_ts = datetime.now()
process_name = "TOP3_CATEGORY_REVENUE_PER_REGION"
user_identity = spark.sparkContext.sparkUser()
execution_start_time = datetime.now()

print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"Load Timestamp: {load_ts}")
print(f"User: {user_identity}")
print(f"Execution Start Time: {execution_start_time}")

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
            (process_name, run_id, status, current_timestamp(), message)
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
try:
    log_to_control_table(process_name, run_id, "STARTED", "Process initiated")
    
    # ===============================================================================================
    # STEP 1: FILTER SALES FOR LAST 12 MONTHS
    # ===============================================================================================
    print("\n[STEP 1] Filtering sales for last 12 months...")
    
    # Read source tables
    sales_df = spark.table("sales")
    products_df = spark.table("products")
    regions_df = spark.table("regions")
    
    # Calculate date threshold (12 months ago)
    date_threshold = add_months(current_date(), -12)
    
    # Filter and join data
    filtered_sales_df = sales_df \
        .filter(col("order_date") >= date_threshold) \
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
    
    row_count_filtered = filtered_sales_df.count()
    print(f"Filtered Sales Row Count: {row_count_filtered}")
    
    # Write to temporary Delta table
    filtered_sales_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("tmp_filtered_sales")
    
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
    
    row_count_aggregated = category_revenue_df.count()
    print(f"Category Revenue Row Count: {row_count_aggregated}")
    
    # Write to temporary Delta table
    category_revenue_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("tmp_category_revenue")
    
    # ===============================================================================================
    # STEP 3: RANK CATEGORIES BY REVENUE WITHIN EACH REGION
    # ===============================================================================================
    print("\n[STEP 3] Ranking categories by revenue within each region...")
    
    # Define window specification
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    row_count_ranked = ranked_categories_df.count()
    print(f"Ranked Categories Row Count: {row_count_ranked}")
    
    # Write to temporary Delta table
    ranked_categories_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("tmp_ranked_categories")
    
    # ===============================================================================================
    # STEP 4: INSERT TOP 3 PER REGION INTO VERSIONED TABLE
    # ===============================================================================================
    print("\n[STEP 4] Inserting top 3 categories per region into versioned table...")
    
    # Filter top 3 and add metadata columns
    top3_df = ranked_categories_df \
        .filter(col("category_rank") <= 3) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("load_ts", lit(load_ts)) \
        .withColumn("load_date", current_timestamp()) \
        .withColumn("update_date", current_timestamp()) \
        .withColumn("source_system", lit("HIVE_MIGRATION")) \
        .select(
            "run_id",
            "region_id",
            "category_id",
            "total_revenue",
            "avg_order_value",
            "category_rank",
            "load_ts",
            "load_date",
            "update_date",
            "source_system"
        ) \
        .orderBy("region_id", "category_rank")
    
    final_row_count = top3_df.count()
    print(f"Final Top 3 Categories Row Count: {final_row_count}")
    
    # Write to versioned Delta table
    top3_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("top3_category_revenue_region_versions")
    
    # ===============================================================================================
    # LOG SUCCESS
    # ===============================================================================================
    execution_end_time = datetime.now()
    processing_time = (execution_end_time - execution_start_time).total_seconds()
    
    success_message = f"Data successfully inserted into versioned results table. Rows: {final_row_count}, Processing Time: {processing_time}s"
    log_to_control_table(process_name, run_id, "SUCCESS", success_message)
    
    # ===============================================================================================
    # DISPLAY SUMMARY
    # ===============================================================================================
    print("\n" + "="*80)
    print("EXECUTION SUMMARY")
    print("="*80)
    print(f"Process Name: {process_name}")
    print(f"Run ID: {run_id}")
    print(f"User: {user_identity}")
    print(f"Start Time: {execution_start_time}")
    print(f"End Time: {execution_end_time}")
    print(f"Processing Time: {processing_time} seconds")
    print(f"Filtered Sales Rows: {row_count_filtered}")
    print(f"Aggregated Category Revenue Rows: {row_count_aggregated}")
    print(f"Ranked Categories Rows: {row_count_ranked}")
    print(f"Final Top 3 Rows Inserted: {final_row_count}")
    print(f"Status: SUCCESS")
    print("="*80)
    
    # Display sample results
    print("\nSample Top 3 Results:")
    top3_df.show(10, truncate=False)
    
    # Unpersist cached dataframe
    filtered_sales_df.unpersist()
    
except Exception as e:
    # ===============================================================================================
    # ERROR HANDLING AND LOGGING
    # ===============================================================================================
    execution_end_time = datetime.now()
    processing_time = (execution_end_time - execution_start_time).total_seconds()
    
    error_message = f"Process failed with error: {str(e)}"
    error_trace = traceback.format_exc()
    
    print("\n" + "="*80)
    print("EXECUTION FAILED")
    print("="*80)
    print(f"Error: {error_message}")
    print(f"\nFull Trace:\n{error_trace}")
    print("="*80)
    
    log_to_control_table(process_name, run_id, "FAILED", error_message)
    
    raise e

# COMMAND ----------

# ===============================================================================================
# COST TRACKING (PLACEHOLDER)
# ===============================================================================================
print("\n[COST TRACKING]")
print("Estimated API Cost: $0.00 (Databricks compute cost calculated separately)")
print("Note: Actual compute cost depends on cluster configuration and execution time.")
