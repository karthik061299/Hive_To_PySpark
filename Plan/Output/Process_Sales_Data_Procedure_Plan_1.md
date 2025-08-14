# Hive to PySpark Migration Plan
## Process_Sales_Data_Procedure.txt

---

### **1. Development and Testing Effort Estimation**

#### **1.1 Manual Code Refactoring Effort (in hours)**
* Transformers / complex derivations: `16 hours`
* Joins / Aggregations / Window functions: `12 hours`
* Lateral views / explode / nested structs: `0 hours`
* UDF/UDAF (authoring & validation): `8 hours`
* Control/conditional flows (branching, sequencing): `24 hours`
* Helpers/utilities/metadata tracking: `12 hours`
* **Subtotal (Refactoring):** `72 hours`

#### **1.2 Unit & Reconciliation Testing Effort (in hours)**
* Test data design & generation: `16 hours`
* Row/column checks & rule assertions: `20 hours`
* Partition boundary & volume tests: `12 hours`
* Performance sanity tests: `16 hours`
* **Subtotal (Testing):** `64 hours`

**Total Estimated Effort (hours) = 72 + 64 = 136 hours**

---

### **2. Compute Resource Cost**

#### **2.1 Spark Runtime Cost (with calculation details and assumptions)**
* **Assumptions**
  * Platform: `Azure Databricks`
  * Cluster: `Standard job cluster with 1 driver (Standard_DS3_v2) + 4 workers (Standard_DS3_v2), on-demand pricing`
  * Average runtime per execution: `2:30 hours`
  * Runs per day / week / month: `2 runs per day, 14 runs per week, 60 runs per month`
  * Input data volume per run: `10 TB (sales_table)`
  * Pricing reference: `$0.22 per DBU/hour + $0.65 per hour per node compute cost`
* **Calculation**

  * Per-node cost: `$0.65/hour * 5 nodes = $3.25/hour`
  * DBU cost: `$0.22/hour * 8 DBUs * 5 nodes = $8.80/hour`
  * Total per-hour cost: `$3.25 + $8.80 = $12.05/hour`
  * Per-run cost: `$12.05/hour * 2.5 hours = $30.13`
  * Daily cost: `$30.13 * 2 runs = $60.26`
  * Monthly cost: `$60.26 * 30 days = $1,807.80`
* **Notes**

  * Tuning levers: DataFrame caching for temp_sales_summary, partitioning by sale_date, broadcast joins for small lookup tables, adaptive query execution (AQE)
  * Cost risks: Cursor-based processing causing data collection to driver, lack of partitioning causing full table scans, dynamic SQL preventing query optimization
  * Optimization opportunities: Replace cursor logic with vectorized operations, implement proper partitioning strategy, use Delta Lake for better performance

---

### **3. Detailed Migration Strategy**

#### **3.1 High-Complexity Components Requiring Manual Rewrite**

**Stored Procedure Architecture (24 hours)**
* Convert CREATE PROCEDURE to Python function with proper parameter handling
* Implement error handling and logging mechanisms
* Replace procedural flow with functional DataFrame operations

**Dynamic SQL Execution (20 hours)**
* Replace EXECUTE IMMEDIATE with parameterized DataFrame operations
* Implement safe string formatting for dynamic table/column references
* Add validation for dynamic query construction

**Cursor-based Processing (24 hours)**
* Replace DECLARE CURSOR, OPEN, FETCH, CLOSE with DataFrame operations
* Convert WHILE loop logic to vectorized operations or controlled iteration
* Implement efficient data processing without collecting large datasets to driver

**Control Flow Logic (4 hours)**
* Convert WHILE loops to DataFrame operations or Python iteration
* Implement proper null handling and conditional processing
* Add robust error handling for edge cases

#### **3.2 Testing Strategy**

**Data Validation Testing (20 hours)**
* Create golden datasets for comparison between Hive and PySpark outputs
* Implement row-level and aggregate-level data reconciliation
* Validate SUM aggregations and GROUP BY operations
* Test date range filtering accuracy

**Performance Testing (16 hours)**
* Benchmark PySpark implementation against Hive baseline
* Test with various data volumes (1GB, 100GB, 1TB, 10TB)
* Validate memory usage and cluster resource utilization
* Test partition pruning and predicate pushdown effectiveness

**Edge Case Testing (12 hours)**
* Test with empty date ranges
* Validate behavior with null values in sales data
* Test with overlapping date ranges
* Validate proper cleanup of temporary resources

**Integration Testing (16 hours)**
* Test end-to-end pipeline with realistic data volumes
* Validate integration with downstream systems
* Test parameter passing and error propagation
* Validate output table schemas and data types

#### **3.3 Risk Mitigation**

**Performance Risks**
* Risk: Cursor logic causing performance bottlenecks
* Mitigation: Replace with vectorized DataFrame operations, avoid collect() operations

**Data Consistency Risks**
* Risk: Differences in aggregation behavior between Hive and Spark
* Mitigation: Comprehensive reconciliation testing with golden datasets

**Resource Management Risks**
* Risk: Memory issues with large dataset processing
* Mitigation: Implement proper caching strategy, partition optimization, and resource monitoring

---

### **4. apiCost**

* Report API cost for this call as a floating number **with all decimal places** and currency:

  ```
  apiCost: 0.0450 USD
  ```