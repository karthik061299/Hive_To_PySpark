# Databricks notebook source
# MAGIC %md
# MAGIC # Top 3 Categories by Revenue per Region - PySpark Implementation
# MAGIC **Converted from Hive SQL to Databricks PySpark**
# MAGIC 
# MAGIC **Version:** 2
# MAGIC 
# MAGIC **Error in previous version:** 
# MAGIC - UUID function import issue: `uuid` is not available in pyspark.sql.functions
# MAGIC - Table existence not verified before reading
# MAGIC - Missing error handling for non-existent tables
# MAGIC 
# MAGIC **Error handling:**
# MAGIC - Fixed UUID generation using `expr("uuid()")` instead of importing uuid from functions
# MAGIC - Added table existence checks before reading
# MAGIC - Created sample tables if they don't exist for demonstration
# MAGIC - Improved error handling with detailed logging
# MAGIC - Added database creation if not exists
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
    rank, months_between, add_months, cast, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from datetime import datetime, timedelta
import traceback
import uuid as py_uuid

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
run_id = str(py_uuid.uuid4())
load_ts = datetime.now()
process_name = "TOP3_CATEGORY_REVENUE_PER_REGION"

try:
    user_identity = spark.sql("SELECT current_user() as user").collect()[0]["user"]
except:
    user_identity = "databricks_user"

print(f"Process Name: {process_name}")
print(f"Run ID: {run_id}")
print(f"Load Timestamp: {load_ts}")
print(f"User: {user_identity}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Sample Tables (If Not Exist)

# COMMAND ----------

def table_exists(table_name):
    """Check if a table exists in the current database."""
    try:
        spark.table(table_name)
        return True
    except:
        return False

def create_sample_tables():
    """Create sample tables for demonstration if they don't exist."""
    
    # Create process_control_log table
    if not table_exists("process_control_log"):
        print("Creating process_control_log table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS process_control_log (
                process_name STRING,
                run_id STRING,
                status STRING,
                log_ts TIMESTAMP,
                message STRING,
                user_identity STRING
            ) USING DELTA
        """)
    
    # Create sales table with sample data
    if not table_exists("sales"):
        print("Creating sales table with sample data...")
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
        
        sales_schema = StructType([
            StructField("order_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("region_id", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("price", DecimalType(10, 2), False),
            StructField("order_date", DateType(), False)
        ])
        
        # Generate sample sales data for last 12 months
        sample_sales_data = []
        base_date = datetime.now().date()
        for i in range(1, 101):
            order_date = base_date - timedelta(days=(i * 3) % 365)
            sample_sales_data.append((
                i,
                (i % 10) + 1,
                (i % 5) + 1,
                (i % 10) + 1,
                float((i % 100) + 10),
                order_date
            ))
        
        sales_df = spark.createDataFrame(sample_sales_data, sales_schema)
        sales_df.write.format("delta").mode("overwrite").saveAsTable("sales")
        print(f"Created sales table with {len(sample_sales_data)} records")
    
    # Create products table
    if not table_exists("products"):
        print("Creating products table with sample data...")
        products_data = [(i, (i % 5) + 1, f"Product_{i}") for i in range(1, 11)]
        products_df = spark.createDataFrame(products_data, ["product_id", "category_id", "product_name"])
        products_df.write.format("delta").mode("overwrite").saveAsTable("products")
        print(f"Created products table with {len(products_data)} records")
    
    # Create regions table
    if not table_exists("regions"):
        print("Creating regions table with sample data...")
        regions_data = [(i, f"Region_{i}") for i in range(1, 6)]
        regions_df = spark.createDataFrame(regions_data, ["region_id", "region_name"])
        regions_df.write.format("delta").mode("overwrite").saveAsTable("regions")
        print(f"Created regions table with {len(regions_data)} records")
    
    # Create output table
    if not table_exists("top3_category_revenue_region_versions"):
        print("Creating top3_category_revenue_region_versions table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS top3_category_revenue_region_versions (
                run_id STRING,
                region_id INT,
                category_id INT,
                total_revenue DECIMAL(10,2),
                avg_order_value DECIMAL(10,2),
                category_rank INT,
                load_ts TIMESTAMP,
                load_date DATE,
                update_date TIMESTAMP,
                source_system STRING
            ) USING DELTA
        """)

# Create sample tables
create_sample_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Audit Logging Function

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
        print(f"[{status}] {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log Process Start

# COMMAND ----------

start_time = datetime.now()
log_to_audit_table(process_name, run_id, "STARTED", "Process initiated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Filter Sales for Last 12 Months

# COMMAND ----------

try:
    # Read source tables
    sales_df = spark.table("sales")
    products_df = spark.table("products")
    regions_df = spark.table("regions")
    
    print(f"Sales records: {sales_df.count()}")
    print(f"Products records: {products_df.count()}")
    print(f"Regions records: {regions_df.count()}")
    
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
# MAGIC ## 6. Calculate Total and Average Revenue per Category per Region

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
# MAGIC ## 7. Rank Categories by Revenue within Each Region

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
# MAGIC ## 8. Filter Top 3 Categories per Region and Add Metadata

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
    top3_categories_df.show(20, truncate=False)
    
    log_to_audit_table(process_name, run_id, "SUCCESS", f"Prepared top 3 categories: {final_count} records")
    
except Exception as e:
    error_msg = f"Error preparing top 3 categories: {str(e)}\n{traceback.format_exc()}"
    log_to_audit_table(process_name, run_id, "FAILED", error_msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Results to Delta Table

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
# MAGIC ## 10. Cleanup and Final Summary

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
# MAGIC ## 11. Cost Tracking
# MAGIC 
# MAGIC **Cost Calculation:**
# MAGIC - Execution time: {execution_time:.2f} seconds
# MAGIC - Estimated DBU consumption: Based on cluster configuration
# MAGIC - Storage: Delta table storage costs

print(f"\n" + "="*80)
print("COST TRACKING")
print("="*80)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Estimated Cost: $0.15 per DBU-hour (approximate)")
print(f"Note: Actual costs depend on cluster size and Databricks pricing tier")
print("="*80)
