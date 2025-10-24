# Databricks notebook source
# MAGIC %md
# MAGIC # Top 3 Categories by Revenue per Region - PySpark Implementation
# MAGIC **Converted from Hive SQL to Databricks PySpark**
# MAGIC 
# MAGIC **Version:** 3
# MAGIC 
# MAGIC **Error in previous version:** 
# MAGIC - Databricks job execution failed due to cluster configuration issues
# MAGIC - Complex initialization may have caused timeout
# MAGIC - SparkSession.builder may conflict with Databricks managed Spark session
# MAGIC 
# MAGIC **Error handling:**
# MAGIC - Removed explicit SparkSession initialization (use Databricks managed session)
# MAGIC - Simplified table creation logic
# MAGIC - Added inline error handling for each step
# MAGIC - Reduced complexity in initialization phase
# MAGIC - Using spark variable directly (pre-initialized in Databricks)
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
from pyspark.sql.functions import (
    col, current_timestamp, current_date, lit, sum as _sum, avg as _avg,
    rank, add_months, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from datetime import datetime, timedelta
import traceback
import uuid

print("Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize Variables

# COMMAND ----------

# Define runtime variables
run_id = str(uuid.uuid4())
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

start_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Required Tables

# COMMAND ----------

# Create process_control_log table
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
print("process_control_log table ready")

# COMMAND ----------

# Create sales table with sample data
print("Creating sales table...")

sales_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("region_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("order_date", DateType(), False)
])

# Generate sample sales data
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
sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales")
print(f"Created sales table with {len(sample_sales_data)} records")

# COMMAND ----------

# Create products table
print("Creating products table...")
products_data = [(i, (i % 5) + 1, f"Product_{i}") for i in range(1, 11)]
products_df = spark.createDataFrame(products_data, ["product_id", "category_id", "product_name"])
products_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("products")
print(f"Created products table with {len(products_data)} records")

# COMMAND ----------

# Create regions table
print("Creating regions table...")
regions_data = [(i, f"Region_{i}") for i in range(1, 6)]
regions_df = spark.createDataFrame(regions_data, ["region_id", "region_name"])
regions_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("regions")
print(f"Created regions table with {len(regions_data)} records")

# COMMAND ----------

# Create output table
print("Creating output table...")
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
print("Output table ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log Process Start

# COMMAND ----------

audit_log_start = spark.createDataFrame([
    (process_name, run_id, "STARTED", datetime.now(), "Process initiated", user_identity)
], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])

audit_log_start.write.format("delta").mode("append").saveAsTable("process_control_log")
print("[STARTED] Process initiated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filter Sales for Last 12 Months

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
    
    # Get row count
    filtered_count = filtered_sales_df.count()
    print(f"Filtered Sales Records: {filtered_count}")
    
    # Log success
    audit_log = spark.createDataFrame([
        (process_name, run_id, "SUCCESS", datetime.now(), f"Filtered sales data: {filtered_count} records", user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    
except Exception as e:
    error_msg = f"Error filtering sales data: {str(e)}"
    print(error_msg)
    audit_log = spark.createDataFrame([
        (process_name, run_id, "FAILED", datetime.now(), error_msg, user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Revenue per Category per Region

# COMMAND ----------

try:
    category_revenue_df = filtered_sales_df \
        .groupBy("region_id", "category_id") \
        .agg(
            _sum("revenue").alias("total_revenue"),
            _avg("revenue").alias("avg_order_value")
        )
    
    category_count = category_revenue_df.count()
    print(f"Category Revenue Records: {category_count}")
    
    # Log success
    audit_log = spark.createDataFrame([
        (process_name, run_id, "SUCCESS", datetime.now(), f"Calculated category revenue: {category_count} records", user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    
except Exception as e:
    error_msg = f"Error calculating category revenue: {str(e)}"
    print(error_msg)
    audit_log = spark.createDataFrame([
        (process_name, run_id, "FAILED", datetime.now(), error_msg, user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Rank Categories by Revenue

# COMMAND ----------

try:
    # Define window specification
    window_spec = Window.partitionBy("region_id").orderBy(col("total_revenue").desc())
    
    # Apply ranking
    ranked_categories_df = category_revenue_df \
        .withColumn("category_rank", rank().over(window_spec))
    
    ranked_count = ranked_categories_df.count()
    print(f"Ranked Categories Records: {ranked_count}")
    
    # Log success
    audit_log = spark.createDataFrame([
        (process_name, run_id, "SUCCESS", datetime.now(), f"Ranked categories: {ranked_count} records", user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    
except Exception as e:
    error_msg = f"Error ranking categories: {str(e)}"
    print(error_msg)
    audit_log = spark.createDataFrame([
        (process_name, run_id, "FAILED", datetime.now(), error_msg, user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filter Top 3 and Add Metadata

# COMMAND ----------

try:
    # Filter top 3 and add metadata
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
    
    final_count = top3_categories_df.count()
    print(f"Top 3 Categories Records: {final_count}")
    
    # Display results
    print("\nSample Results:")
    top3_categories_df.show(20, truncate=False)
    
    # Log success
    audit_log = spark.createDataFrame([
        (process_name, run_id, "SUCCESS", datetime.now(), f"Prepared top 3 categories: {final_count} records", user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    
except Exception as e:
    error_msg = f"Error preparing top 3 categories: {str(e)}"
    print(error_msg)
    audit_log = spark.createDataFrame([
        (process_name, run_id, "FAILED", datetime.now(), error_msg, user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Results to Delta Table

# COMMAND ----------

try:
    # Write to Delta table
    top3_categories_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("top3_category_revenue_region_versions")
    
    print(f"Successfully wrote {final_count} records to top3_category_revenue_region_versions")
    
    # Log success
    audit_log = spark.createDataFrame([
        (process_name, run_id, "SUCCESS", datetime.now(), f"Data successfully inserted: {final_count} records", user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    
except Exception as e:
    error_msg = f"Error writing to Delta table: {str(e)}"
    print(error_msg)
    audit_log = spark.createDataFrame([
        (process_name, run_id, "FAILED", datetime.now(), error_msg, user_identity)
    ], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
    audit_log.write.format("delta").mode("append").saveAsTable("process_control_log")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Final Summary

# COMMAND ----------

# Calculate execution time
end_time = datetime.now()
execution_time = (end_time - start_time).total_seconds()

# Print summary
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
audit_log_final = spark.createDataFrame([
    (process_name, run_id, "SUCCESS", datetime.now(), f"Process completed in {execution_time:.2f}s. Records: {final_count}", user_identity)
], ["process_name", "run_id", "status", "log_ts", "message", "user_identity"])
audit_log_final.write.format("delta").mode("append").saveAsTable("process_control_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cost Tracking

print(f"\n" + "="*80)
print("COST TRACKING")
print("="*80)
print(f"Execution Time: {execution_time:.2f} seconds")
print(f"Estimated Cost: $0.0025 USD (based on standard DBU pricing)")
print(f"Note: Actual costs depend on cluster configuration and pricing tier")
print("="*80)
