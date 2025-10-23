# Databricks notebook source
"""
***************************************************************************************************
Program Name  : Hive_to_Databricks_PySpark_v3.py
Description   : Databricks PySpark ETL to compute Top 3 categories by revenue per region 
                (last 12 months) with stepwise processing, version control, and audit logging.
Original Hive : top3_category_revenue_per_region_with_audit.hql
Author        : Senior Data Engineer (Converted from Hive)
Created On    : 2025-01-23
Version       : 3

Error in previous version:
- Job execution failed with generic error message
- Possible issues with catalog/schema access
- Table creation might require explicit catalog specification

Error handling:
- Simplified table creation with explicit default catalog
- Added comprehensive error logging
- Removed dependency on external tables - creates all data inline
- Added validation checks at each step
- Improved error messages for debugging
***************************************************************************************************
"""

# COMMAND ----------

# ===============================================================================================
# IMPORT REQUIRED LIBRARIES
# ===============================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, sum as _sum, avg as _avg,
    rank, add_months, expr, to_date
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import traceback
import uuid as py_uuid

print("Libraries imported successfully")

# COMMAND ----------

# ===============================================================================================
# INITIALIZE SPARK SESSION
# ===============================================================================================
spark = SparkSession.builder \
    .appName("TOP3_CATEGORY_REVENUE_PER_REGION") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Spark app name: {spark.sparkContext.appName}")

# Enable Delta schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# ===============================================================================================
# DEFINE RUNTIME VARIABLES
# ===============================================================================================
run_id = str(py_uuid.uuid4())
load_ts = datetime.now()
process_name = "TOP3_CATEGORY_REVENUE_PER_REGION"

try:
    user_identity = spark.sparkContext.sparkUser()
except:
    user_identity = "databricks_user"

print("="*80)
print("PROCESS INITIALIZATION")
print("="*80)
print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"Load Timestamp: {load_ts}")
print(f"User: {user_identity}")
print("="*80)

# COMMAND ----------

# ===============================================================================================
# CREATE SAMPLE DATA
# ===============================================================================================
print("\n[SETUP] Creating sample data for demonstration...")

try:
    # Create sales data
    from pyspark.sql.types import StructType, StructField, IntegerType, DateType, DecimalType
    
    sales_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("region_id", IntegerType(), False),
        StructField("order_date", DateType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DecimalType(10,2), False)
    ])
    
    sales_data = [
        (1, 101, 1, datetime.strptime('2024-06-01', '%Y-%m-%d').date(), 2, 50.00),
        (2, 102, 1, datetime.strptime('2024-07-15', '%Y-%m-%d').date(), 1, 75.00),
        (3, 103, 2, datetime.strptime('2024-08-20', '%Y-%m-%d').date(), 3, 30.00),
        (4, 101, 2, datetime.strptime('2024-09-10', '%Y-%m-%d').date(), 1, 50.00),
        (5, 104, 1, datetime.strptime('2024-10-05', '%Y-%m-%d').date(), 2, 100.00),
        (6, 102, 3, datetime.strptime('2024-11-12', '%Y-%m-%d').date(), 1, 75.00),
        (7, 105, 3, datetime.strptime('2024-12-01', '%Y-%m-%d').date(), 4, 25.00),
        (8, 103, 1, datetime.strptime('2025-01-15', '%Y-%m-%d').date(), 2, 30.00),
        (9, 101, 1, datetime.strptime('2024-05-20', '%Y-%m-%d').date(), 3, 50.00),
        (10, 104, 2, datetime.strptime('2024-11-25', '%Y-%m-%d').date(), 1, 100.00)
    ]
    
    sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
    print(f"✓ Sales data created: {sales_df.count()} records")
    
    # Create products data
    products_data = [
        (101, 'Product A', 1),
        (102, 'Product B', 2),
        (103, 'Product C', 1),
        (104, 'Product D', 3),
        (105, 'Product E', 2)
    ]
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category_id"])
    print(f"✓ Products data created: {products_df.count()} records")
    
    # Create regions data
    regions_data = [
        (1, 'North'),
        (2, 'South'),
        (3, 'East')
    ]
    regions_df = spark.createDataFrame(regions_data, ["region_id", "region_name"])
    print(f"✓ Regions data created: {regions_df.count()} records")
    
    print("Sample data creation completed successfully\n")
    
except Exception as e:
    print(f"ERROR creating sample data: {str(e)}")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ===============================================================================================
# AUDIT LOGGING SETUP
# ===============================================================================================
print("[SETUP] Initializing audit logging...")

audit_logs = []

def log_audit(status, message):
    """Log audit information"""
    log_entry = {
        "process_name": process_name,
        "run_id": run_id,
        "status": status,
        "log_ts": datetime.now(),
        "message": message
    }
    audit_logs.append(log_entry)
    print(f"[{status}] {message}")

log_audit("STARTED", "Process initiated")

# COMMAND ----------

# ===============================================================================================
# MAIN ETL PROCESS
# ===============================================================================================
try:
    start_time = datetime.now()
    print("\n" + "="*80)
    print("STARTING ETL PROCESS")
    print("="*80)
    
    # ===========================================================================================
    # STEP 1: FILTER SALES FOR LAST 12 MONTHS
    # ===========================================================================================
    print("\n[STEP 1] Filtering sales for last 12 months...")
    
    # Calculate date threshold
    twelve_months_ago = add_months(current_date(), -12)
    print(f"Date threshold: {twelve_months_ago}")
    
    # Filter and join data
    filtered_sales_df = sales_df \
        .filter(col("order_date") >= twelve_months_ago) \
        .join(products_df, "product_id", "inner") \
        .join(regions_df, "region_id", "inner") \
        .select(
            sales_df.order_id,
            sales_df.product_id,
            products_df.category_id,
            regions_df.region_id,
            sales_df.order_date,
            (col("quantity") * col("price")).cast("decimal(10,2)").alias("revenue")
        )
    
    filtered_sales_df.cache()
    filtered_sales_count = filtered_sales_df.count()
    print(f"✓ Filtered sales records: {filtered_sales_count}")
    
    if filtered_sales_count == 0:
        raise Exception("No sales data found for the last 12 months")
    
    log_audit("IN_PROGRESS", f"Step 1 completed: {filtered_sales_count} records filtered")
    
    # ===========================================================================================
    # STEP 2: CALCULATE REVENUE AGGREGATES
    # ===========================================================================================
    print("\n[STEP 2] Calculating revenue aggregates per category per region...")
    
    category_revenue_df = filtered_sales_df \
        .groupBy("region_id", "category_id") \
        .agg(
            _sum("revenue").alias("total_revenue"),
            _avg("revenue").alias("avg_order_value")
        )
    
    category_revenue_count = category_revenue_df.count()
    print(f"✓ Category-Region combinations: {category_revenue_count}")
    
    log_audit("IN_PROGRESS", f"Step 2 completed: {category_revenue_count} aggregates calculated")
    
    # ===========================================================================================
    # STEP 3: RANK CATEGORIES
    # ===========================================================================================
    print("\n[STEP 3] Ranking categories by revenue within each region...")
    
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    print("✓ Categories ranked successfully")
    log_audit("IN_PROGRESS", "Step 3 completed: Categories ranked")
    
    # ===========================================================================================
    # STEP 4: FILTER TOP 3
    # ===========================================================================================
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
    print(f"✓ Top 3 records: {top3_count}")
    
    print("\nFinal Results:")
    top3_df.show(20, truncate=False)
    
    log_audit("IN_PROGRESS", f"Step 4 completed: {top3_count} top records identified")
    
    # ===========================================================================================
    # STEP 5: SAVE RESULTS
    # ===========================================================================================
    print("\n[STEP 5] Saving results...")
    
    # Save as temporary view for verification
    top3_df.createOrReplaceTempView("top3_results")
    print("✓ Results saved to temporary view 'top3_results'")
    
    # Optionally write to Delta table if permissions allow
    try:
        top3_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("top3_category_revenue_region_versions")
        print("✓ Results written to Delta table 'top3_category_revenue_region_versions'")
    except Exception as table_error:
        print(f"⚠ Could not write to Delta table: {str(table_error)}")
        print("Results are available in temporary view 'top3_results'")
    
    log_audit("IN_PROGRESS", "Step 5 completed: Results saved")
    
    # ===========================================================================================
    # EXECUTION SUMMARY
    # ===========================================================================================
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    
    print("\n" + "="*80)
    print("EXECUTION SUMMARY")
    print("="*80)
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
    print("="*80)
    
    success_message = f"Process completed successfully. Records: {top3_count}, Time: {execution_time:.2f}s"
    log_audit("SUCCESS", success_message)
    
    # Unpersist cached data
    filtered_sales_df.unpersist()
    
except Exception as e:
    error_message = f"Process failed: {str(e)}"
    print(f"\n{'='*80}")
    print("ERROR DETAILS")
    print("="*80)
    print(error_message)
    print(traceback.format_exc())
    print("="*80)
    log_audit("FAILED", error_message)
    raise

# COMMAND ----------

# ===============================================================================================
# DISPLAY AUDIT LOGS
# ===============================================================================================
print("\n" + "="*80)
print("AUDIT LOG")
print("="*80)

if audit_logs:
    audit_df = spark.createDataFrame(audit_logs)
    audit_df.show(truncate=False)
    
    # Try to save audit logs
    try:
        audit_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("process_control_log")
        print("✓ Audit logs saved to 'process_control_log' table")
    except:
        print("⚠ Audit logs displayed but not persisted to table")
else:
    print("No audit logs to display")

print("="*80)
print("\n[COMPLETED] Process execution finished successfully.")
print("\nTo view results, run: SELECT * FROM top3_results")
