# Databricks notebook source
"""
***************************************************************************************************
Program Name  : Hive_to_Databricks_PySpark_v1.py
Description   : Databricks PySpark ETL to compute Top 3 categories by revenue per region 
                (last 12 months) with stepwise processing, version control, and audit logging.
Original File : top3_category_revenue_per_region_with_audit.hql
Author        : Senior Data Engineer (Converted from Hive)
Created On    : 2025-01-23
Version       : 1
***************************************************************************************************
"""

# COMMAND ----------

# ===============================================================================================
# IMPORT LIBRARIES
# ===============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg as _avg, current_timestamp, current_date, 
    lit, rank, months_between, add_months, expr, uuid
)
from pyspark.sql.window import Window
from datetime import datetime
import traceback

# COMMAND ----------

# ===============================================================================================
# INITIALIZE SPARK SESSION
# ===============================================================================================
spark = SparkSession.builder.appName("TOP3_CATEGORY_REVENUE_PER_REGION").getOrCreate()

# Enable Delta schema evolution if needed
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# ===============================================================================================
# DEFINE RUNTIME VARIABLES
# ===============================================================================================
import uuid as uuid_lib

RUN_ID = str(uuid_lib.uuid4())
LOAD_TS = datetime.now()
PROCESS_NAME = 'TOP3_CATEGORY_REVENUE_PER_REGION'

print(f"Process Name: {PROCESS_NAME}")
print(f"Run ID: {RUN_ID}")
print(f"Load Timestamp: {LOAD_TS}")

# COMMAND ----------

# ===============================================================================================
# AUDIT LOGGING FUNCTION
# ===============================================================================================
def log_process_status(process_name, run_id, status, message):
    """
    Logs process execution status to the process_control_log table.
    
    Args:
        process_name (str): Name of the process
        run_id (str): Unique run identifier
        status (str): Status of the process (STARTED, SUCCESS, FAILED)
        message (str): Descriptive message
    """
    try:
        log_df = spark.createDataFrame([
            (process_name, run_id, status, datetime.now(), message)
        ], ["process_name", "run_id", "status", "log_ts", "message"])
        
        # Write to Delta table (append mode)
        log_df.write.format("delta").mode("append").saveAsTable("process_control_log")
        print(f"[{status}] {message}")
    except Exception as e:
        print(f"Error logging to process_control_log: {str(e)}")

# COMMAND ----------

# ===============================================================================================
# LOG PROCESS START
# ===============================================================================================
log_process_status(PROCESS_NAME, RUN_ID, "STARTED", "Process initiated")

# COMMAND ----------

try:
    # ===============================================================================================
    # STEP 1: FILTER SALES FOR LAST 12 MONTHS
    # ===============================================================================================
    print("\n=== STEP 1: Filtering sales for last 12 months ===")
    
    # Read source tables
    sales_df = spark.table("sales")
    products_df = spark.table("products")
    regions_df = spark.table("regions")
    
    # Calculate date 12 months ago
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
    
    row_count_step1 = filtered_sales_df.count()
    print(f"Filtered sales records: {row_count_step1}")
    
    # Write intermediate result as Delta table
    filtered_sales_df.write.format("delta").mode("overwrite").saveAsTable("tmp_filtered_sales")
    
    # COMMAND ----------
    
    # ===============================================================================================
    # STEP 2: CALCULATE TOTAL AND AVERAGE REVENUE PER CATEGORY PER REGION
    # ===============================================================================================
    print("\n=== STEP 2: Calculating revenue aggregates ===")
    
    category_revenue_df = filtered_sales_df \
        .groupBy("region_id", "category_id") \
        .agg(
            _sum("revenue").alias("total_revenue"),
            _avg("revenue").alias("avg_order_value")
        )
    
    row_count_step2 = category_revenue_df.count()
    print(f"Category-Region combinations: {row_count_step2}")
    
    # Write intermediate result
    category_revenue_df.write.format("delta").mode("overwrite").saveAsTable("tmp_category_revenue")
    
    # COMMAND ----------
    
    # ===============================================================================================
    # STEP 3: RANK CATEGORIES BY REVENUE WITHIN EACH REGION
    # ===============================================================================================
    print("\n=== STEP 3: Ranking categories by revenue ===")
    
    # Define window specification
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    row_count_step3 = ranked_categories_df.count()
    print(f"Ranked categories: {row_count_step3}")
    
    # Write intermediate result
    ranked_categories_df.write.format("delta").mode("overwrite").saveAsTable("tmp_ranked_categories")
    
    # COMMAND ----------
    
    # ===============================================================================================
    # STEP 4: INSERT TOP 3 PER REGION INTO VERSIONED TABLE
    # ===============================================================================================
    print("\n=== STEP 4: Inserting top 3 categories per region ===")
    
    # Filter top 3 and add metadata
    top3_df = ranked_categories_df \
        .filter(col("category_rank") <= 3) \
        .withColumn("run_id", lit(RUN_ID)) \
        .withColumn("load_ts", lit(LOAD_TS)) \
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
    
    final_row_count = top3_df.count()
    print(f"Top 3 records to insert: {final_row_count}")
    
    # Write to versioned Delta table
    top3_df.write.format("delta").mode("append").saveAsTable("top3_category_revenue_region_versions")
    
    # COMMAND ----------
    
    # ===============================================================================================
    # CLEANUP TEMPORARY TABLES
    # ===============================================================================================
    print("\n=== Cleaning up temporary tables ===")
    spark.sql("DROP TABLE IF EXISTS tmp_filtered_sales")
    spark.sql("DROP TABLE IF EXISTS tmp_category_revenue")
    spark.sql("DROP TABLE IF EXISTS tmp_ranked_categories")
    
    # Unpersist cached data
    filtered_sales_df.unpersist()
    
    # COMMAND ----------
    
    # ===============================================================================================
    # LOG SUCCESS
    # ===============================================================================================
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
    # ===============================================================================================
    # ERROR HANDLING AND LOGGING
    # ===============================================================================================
    error_message = f"Process failed with error: {str(e)}\n{traceback.format_exc()}"
    log_process_status(PROCESS_NAME, RUN_ID, "FAILED", error_message)
    
    print("\n" + "="*80)
    print("PROCESS FAILED")
    print("="*80)
    print(error_message)
    print("="*80)
    
    raise e

# COMMAND ----------

# ===============================================================================================
# END OF NOTEBOOK
# ===============================================================================================
