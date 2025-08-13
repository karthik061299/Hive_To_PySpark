=============================================
Author:        Ascendion AVA+
Created on:
Description:   Hive script to demonstrate database and table management, data transformation, and analysis in a retail context.
=============================================

### **1. Overview of Job**
This Hive job script provides a comprehensive demonstration of data processing within a retail analytics scenario. The primary purpose is to create a structured data environment, ingest raw data into various types of tables, perform transformations and aggregations, and derive insights.

The job aligns with standard enterprise data processing by showcasing foundational data warehousing concepts such as:
*   **Data Structuring:** Creating a dedicated database (`retail_db`) to encapsulate all related data objects.
*   **Data Ingestion:** Defining schemas for external tables (`customers`, `orders`) to read raw data.
*   **Data Optimization:** Utilizing partitioned (`sales`) and bucketed (`transactions`) tables to improve query performance.
*   **Data Transformation:** Implementing dynamic partitioning to load transformed data into the `sales` table.
*   **Business Logic:** Applying aggregations (`GROUP BY`), joins, and conditional logic (`CASE` statement) to analyze sales data and identify frequent customers.
*   **Extensibility:** Using a User-Defined Function (UDF) to perform custom data manipulation (`reverse_string`).
*   **Data Abstraction:** Creating a view (`frequent_customers`) to simplify complex queries for end-users.

The business process supported is a typical retail data analysis workflow, where raw transaction and customer data is processed to generate aggregated reports, such as sales performance by region and identification of high-value customers.

### **2. Job Structure and Design**
The script follows a clear, sequential structure, executing a series of DDL (Data Definition Language) and DML (Data Manipulation Language) statements.

*   **Setup:**
    1.  `CREATE DATABASE`: A database named `retail_db` is created to logically group all subsequent objects.
    2.  `USE`: Sets the context to the newly created database.

*   **Table Creation (DDL):**
    1.  `CREATE TABLE customers`: Defines an external table for customer master data.
    2.  `CREATE TABLE orders`: Defines an external table for order transaction data.
    3.  `CREATE TABLE sales`: Defines a managed, partitioned table to store aggregated sales data. Partitioning by `region` is a key design choice for performance.
    4.  `CREATE TABLE transactions`: Defines a managed, bucketed table to store raw transaction details. Bucketing by `customer_id` is designed to optimize joins on this key.

*   **Data Transformation and Loading (DML):**
    1.  `SET hive.exec.dynamic.partition...`: Enables dynamic partitioning, allowing Hive to automatically create partitions based on data values.
    2.  `INSERT INTO sales PARTITION (region)`: Data is read from the `transactions` table, transformed using a `CASE` statement to derive the `region`, and loaded into the `sales` table dynamically.

*   **Data Analysis and Querying:**
    1.  `SELECT ... FROM customers JOIN orders`: A simple join to combine customer and order information.
    2.  `SELECT ... FROM sales GROUP BY region`: An aggregation query to calculate total sales and revenue per region.
    3.  `CREATE TEMPORARY FUNCTION`: A UDF is registered for the session to reverse string values.
    4.  `SELECT ... reverse_string(...)`: The UDF is used on the `sales` table.
    5.  `CREATE VIEW frequent_customers`: A view is created to encapsulate the logic for finding customers with more than one order.
    6.  `SELECT * FROM frequent_customers`: The view is queried to retrieve the results.

*   **Cleanup:**
    1.  `DROP VIEW`, `DROP TABLE`, `DROP DATABASE`: The script concludes by systematically removing all created objects to ensure a clean state after execution.

*   **Dependencies:** The script assumes that the source data files for `customers` and `orders` exist in the default HDFS location that Hive reads from. It also depends on the `org.apache.hadoop.hive.ql.udf.generic.GenericUDFReverse` Java class being available in the classpath for the UDF.

### **3. Data Flow and Processing Logic**
The data flow can be visualized as two main paths: one for sales analysis and another for identifying frequent customers.

```
+------------------+      +------------------+
|   Source:        |      |   Source:        |
|   customers      |      |   orders         |
|   (External Table) |      |   (External Table) |
+--------+---------+      +--------+---------+
         |                       |
         +----------+------------+
                    |
                    v
+------------------------------------------+
|   Join and Aggregate (in View)           |
|   - Join on customer_id                  |
|   - Group by customer_name               |
|   - Count orders                         |
+-------------------+----------------------+
                    |
                    v
+------------------------------------------+
|   Output View: frequent_customers        |
+------------------------------------------+


+------------------+
|   Source:        |
|   transactions   |
|   (Bucketed Table) |
+--------+---------+
         |
         v
+------------------------------------------+
|   Transform and Load (Dynamic Partition) |
|   - Map columns (transaction_id -> sale_id) |
|   - Derive 'region' with CASE statement  |
+-------------------+----------------------+
         |
         v
+------------------------------------------+
|   Target: sales                          |
|   (Partitioned Table)                    |
+-------------------+----------------------+
         |
         v
+------------------------------------------+
|   Aggregate and Analyze                  |
|   - Group by region                      |
|   - SUM(sale_amount), COUNT(*)           |
+-------------------+----------------------+
         |
         v
+------------------------------------------+
|   Final Output (Query Result)            |
+------------------------------------------+
```

### **4. Data Mapping**
The primary data mapping occurs during the insertion from the `transactions` table into the `sales` table.

| Target Table Name | Target Column Name | Source Table/Step Name | Source Column Name | Transformation Rule / Business Logic |
|-------------------|--------------------|------------------------|--------------------|---------------------------------------------------------------------------------|
| sales             | sale_id            | transactions           | transaction_id     | Direct mapping.                                                                 |
| sales             | sale_amount        | transactions           | amount             | Direct mapping.                                                                 |
| sales             | sale_date          | transactions           | transaction_date   | Direct mapping.                                                                 |
| sales             | region             | transactions           | customer_id        | `CASE WHEN t.customer_id IN (1, 2) THEN 'north_america' ELSE 'europe' END`      |

### **5. Complexity Analysis**

| Category                   | Measurement  |
| -------------------------- | ------------ |
| Number of Tables Used      | 4            |
| Source/Target Systems      | HDFS/Hive    |
| Transformation Steps       | 3            |
| Parameters Used            | 0            |
| Reusable Components        | 1 (View)     |
| Control Logic              | 1 (CASE)     |
| External Dependencies      | 1 (UDF)      |
| Performance Considerations | Partitioning, Bucketing |
| Volume Handling            | Dynamic Partitions |
| Error Handling             | None explicitly defined |
| Overall Complexity Score   | 85           |

### **6. Key Outputs**
The script is designed for interactive execution and produces several query results as its output. The key outputs are:
1.  **Joined Customer and Order Data:** A result set showing customer names, cities, and their corresponding order IDs and statuses.
2.  **Regional Sales Analysis:** An aggregated table showing the total number of sales and the total sales amount for each region (`north_america`, `europe`).
3.  **Reversed Sale Dates:** A result set from the `sales` table demonstrating the UDF usage, showing `sale_id` and the `sale_date` string reversed.
4.  **Frequent Customers List:** A result set from the `frequent_customers` view, listing customers who have placed more than one order.

The script's final state is a clean environment, as it drops all created tables, the view, and the database, leaving no persistent artifacts.

### **7. API Cost Calculations**
apiCost: 0.0031 USD