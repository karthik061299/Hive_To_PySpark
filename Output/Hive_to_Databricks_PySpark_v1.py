# Databricks notebook source
# MAGIC %md
# MAGIC # Top 3 Categories by Revenue per Region - PySpark Implementation
# MAGIC **Converted from Hive SQL to Databricks PySpark**
# MAGIC 
# MAGIC **Version:** 1
# MAGIC 
# MAGIC **Original Program:** top3_category_revenue_per_region_with_audit.hql
# MAGIC 
# MAGIC **Description:** Compute Top 3 categories by revenue per region (last 12 months)
# MAGIC with stepwise processing, version control, and audit logging.
# MAGIC 
# MAGIC **Author:** Sasmon (Converted by Senior Data Engineer)
# MAGIC 
# MAGIC **Created On:** 2018-10-03
# MAGIC 
# MAGIC **Converted On:** 2025

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, sum as _sum, avg as _avg,
    rank, months_between, add_months, cast, uuid, expr
)
from pyspark.sql.window import Window
from datetime import datetime
import traceback

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize Spark Session and Variables

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("Top3_Category_Revenue_Per_Region").getOrCreate()

# Enable Delta Lake optimizations
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Define runtime variables
run_id = str(uuid.uuid4())
load_ts = datetime.now()
process_name = "TOP3_CATEGORY_REVENUE_PER_REGION"
user_identity = spark.sql("SELECT current_user() as user").collect()[0]["user"]

print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"Load Timestamp: {load_ts}")
print(f"User: {user_identity}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Audit Logging Function

# COMMAND ----------

def log_to_audit_table(process_name, run_id, status, message):
    """
    Log process execution status to audit control table.
    
    Args:
        process_name (str): Name of the process
        run_id (str): Unique run identifier
        status (str): Status of the process (STARTED, SUCCESS, FAILED)
        message (str): Log message
    """
    try:
        audit_log_df = spark.createDataFrame([
            (process_name, run_id, status, datetime.now(), message, user_identity)
        ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
        
        # Write to Delta table (append mode)
        audit_log_df.write.format("delta").mode("append").saveAsTable("process_control_log")
        
        print(f"[{status}] {message}")
    except Exception as e:
        print(f"Error logging to audit table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log Process Start

# COMMAND ----------

start_time = datetime.now()
log_to_audit_table(process_name, run_id, "STARTED", "Process initiated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filter Sales for Last 12 Months

# COMMAND ----------

try:
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
    
    # Get row count for logging
    filtered_count = filtered_sales_df.count()
    print(f"Filtered Sales Records: {filtered_count}")
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Filtered sales data: {filtered_count} records")
    
except Exception as e:
    error_msg = f"Error filtering sales data: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Total and Average Revenue per Category per Region

# COMMAND ----------

try:
    category_revenue_df = filtered_sales_df \
        .groupBy("region_id", "category_id") \
        .agg(
            _sum("revenue").alias("total_revenue"),
            _avg("revenue").alias("avg_order_value")
        )
    
    # Get row count
    category_count = category_revenue_df.count()
    print(f"Category Revenue Records: {category_count}")
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Calculated category revenue: {category_count} records")
    
except Exception as e:
    error_msg = f"Error calculating category revenue: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Rank Categories by Revenue within Each Region

# COMMAND ----------

try:
    # Define window specification for ranking
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    # Apply ranking
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    # Get row count
    ranked_count = ranked_categories_df.count()
    print(f"Ranked Categories Records: {ranked_count}")
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Ranked categories: {ranked_count} records")
    
except Exception as e:
    error_msg = f"Error ranking categories: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filter Top 3 Categories per Region and Add Metadata

# COMMAND ----------

try:
    # Filter top 3 and add metadata columns
    top3_categories_df = ranked_categories_df \
        .filter(col("category_rank") <= 3) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("load_ts", lit(load_ts)) \
        .withColumn("load_date", current_date()) \
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
    
    # Get final row count
    final_count = top3_categories_df.count()
    print(f"Top 3 Categories Records: {final_count}")
    
    # Display sample results
    print("\nSample Results:")
    top3_categories_df.show(10, truncate=False)
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Prepared top 3 categories: {final_count} records")
    
except Exception as e:
    error_msg = f"Error preparing top 3 categories: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Results to Delta Table

# COMMAND ----------

try:
    # Write to versioned Delta table (append mode)
    top3_categories_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("top3_category_revenue_region_versions")
    
    print(f"Successfully wrote {final_count} records to top3_category_revenue_region_versions")
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Data successfully inserted into versioned results table: {final_count} records")
    
except Exception as e:
    error_msg = f"Error writing to Delta table: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cleanup and Final Summary

# COMMAND ----------

# Unpersist cached DataFrames
filtered_sales_df.unpersist()

# Calculate execution time
end_time = datetime.now()
execution_time = (end_time - start_time).total_seconds()

# Final summary
print("\n" + "="*80)
print("EXECUTION SUMMARY")
print("="*80)
print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"User: {user_identity}")
print(f"Start Time: {start_time}")
print(f"End Time: {end_time}")
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Records Processed: {filtered_count}")
print(f"Final Records Written: {final_count}")
print(f"Status: SUCCESS")
print("="*80)

# Log final success
log_to_audit_table(
    process_name, 
    run_id, 
    "SUCCESS", 
    f"Process completed successfully in {execution_time:.2f} seconds. Records written: {final_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cost Tracking (Placeholder)
# MAGIC 
# MAGIC **Note:** Actual cost calculation would require Databricks billing API integration.
# MAGIC Estimated cost factors:
# MAGIC - DBU consumption based on cluster size and execution time
# MAGIC - Storage costs for Delta tables
# MAGIC - API call costs (if applicable)

print(f"\nEstimated Execution Cost: Based on {execution_time:.2f} seconds of cluster runtime")
print("For actual costs, refer to Databricks billing dashboard.")
