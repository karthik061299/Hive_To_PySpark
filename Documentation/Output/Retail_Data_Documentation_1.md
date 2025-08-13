=============================================
Author:        Ascendion AVA+
Created on:
Description:   This document provides a detailed overview of the Hive job for processing retail data.
=============================================

---

### **1. Overview of Job**

This Hive job is designed to process and analyze retail data. It demonstrates a complete data pipeline within Hive, starting from raw data ingestion into tables, followed by transformations, aggregations, and analysis. The job aligns with enterprise data processing practices by structuring data into a dedicated database, using partitioned and bucketed tables for performance optimization, and creating analytical views for business intelligence.

The primary business process supported by this job is sales analysis. It aims to provide insights into sales performance across different geographical regions and to identify high-value, frequent customers based on their order history. The workflow includes creating tables for customers, orders, sales, and transactions, joining these tables to enrich the data, and performing calculations to derive key business metrics.

---

### **2. Job Structure and Design**

The Hive script is a sequential workflow executing a series of SQL-like commands (HQL). The structure follows a logical progression from setup to cleanup.

*   **Setup:**
    *   A database `retail_db` is created to logically group all related tables.
*   **Table Creation:**
    *   `customers`: Stores customer profile information.
    *   `orders`: Stores customer order history.
    *   `sales`: A partitioned table to store sales data by `region`, enabling efficient querying on regional data.
    *   `transactions`: A bucketed table, clustered by `customer_id`, which helps in optimizing joins or sampling on the customer dimension.
*   **Data Transformation and Loading:**
    *   A `JOIN` operation is performed between `customers` and `orders` to link customers with their orders.
    *   Dynamic partitioning is used to insert data from the `transactions` table into the `sales` table, automatically creating partitions based on the `region` column.
*   **Data Analysis and Aggregation:**
    *   A `GROUP BY` query on the `sales` table calculates the total number of sales and the total sales amount for each region.
    *   A temporary User-Defined Function (UDF) `reverse_string` is created and used to demonstrate custom data manipulation.
*   **View Creation:**
    *   A view `frequent_customers` is created to encapsulate the logic for identifying customers who have placed more than one order. This provides a reusable component for querying frequent customers.
*   **Dependencies:**
    *   The script is self-contained in its logic but implicitly depends on external data files for the `customers` and `orders` tables to be loaded into the Hadoop Distributed File System (HDFS) at the correct location.
*   **Cleanup:**
    *   The script concludes by dropping the view, all created tables, and the database to ensure a clean state after execution.

---

### **3. Data Flow and Processing Logic**

The data flows from source tables through various processing steps to produce analytical outputs.

```
+----------------------------------------------+
| [Source Tables]                              |
| Description: Create and define schema for    |
| customers, orders, transactions, and sales.  |
+----------------------------------------------+
                        |
                        ↓
+----------------------------------------------+
| [Join Customers and Orders]                  |
| Description: Combine customer data with      |
| their corresponding order information.        |
+----------------------------------------------+
                        |
                        ↓
+----------------------------------------------+
| [Load Sales Data via Dynamic Partitioning]   |
| Description: Insert transaction data into    |
| the sales table, partitioned by region.      |
+----------------------------------------------+
                        |
                        ↓
+----------------------------------------------+
| [Analyze Sales by Region]                    |
| Description: Aggregate sales data to get     |
| total sales and amount per region.           |
+----------------------------------------------+
                        |
                        ↓
+----------------------------------------------+
| [Identify Frequent Customers]                |
| Description: Create a view to list customers |
| with more than one order.                    |
+----------------------------------------------+
                        |
                        ↓
+----------------------------------------------+
| [Final Outputs]                              |
| Description: Queries on sales analysis and   |
| frequent customers view.                     |
+----------------------------------------------+
```

---

### **4. Data Mapping**

| Target Table Name      | Target Column Name | Source Table/Step Name | Source Column Name | Transformation Rule / Business Logic                               |
| ---------------------- | ------------------ | ---------------------- | ------------------ | ------------------------------------------------------------------ |
| sales                  | sale_id            | transactions           | transaction_id     | Direct mapping.                                                    |
| sales                  | sale_amount        | transactions           | amount             | Direct mapping.                                                    |
| sales                  | sale_date          | transactions           | transaction_date   | Direct mapping.                                                    |
| sales                  | region             | transactions           | customer_id        | `CASE WHEN customer_id IN (1, 2) THEN 'north_america' ELSE 'europe' END` |
| frequent_customers     | customer_name      | customers              | customer_name      | Direct mapping.                                                    |
| frequent_customers     | total_orders       | orders                 | order_id           | `COUNT(o.order_id)` with `GROUP BY c.customer_name` and `HAVING COUNT > 1` |

---

### **5. Complexity Analysis**

| Category                   | Measurement                                                              |
| -------------------------- | ------------------------------------------------------------------------ |
| Number of Tables Used      | 4 tables (`customers`, `orders`, `sales`, `transactions`) and 1 view (`frequent_customers`). |
| Source/Target Systems      | Hive / HDFS.                                                             |
| Transformation Steps       | 5 (Join, Dynamic Partition Insert, Aggregation, UDF, View Creation).     |
| Parameters Used            | 2 (`hive.exec.dynamic.partition`, `hive.exec.dynamic.partition.mode`).   |
| Reusable Components        | 1 view (`frequent_customers`).                                           |
| Control Logic              | `CASE` statement for partitioning logic.                                 |
| External Dependencies      | Implicit dependency on external data files for table loading.            |
| Performance Considerations | Partitioning (`sales` table) and Bucketing (`transactions` table) are used. |
| Volume Handling            | Designed to handle large volumes through partitioning and bucketing.     |
| Error Handling             | `IF NOT EXISTS` and `IF EXISTS` clauses provide basic error handling for object creation/deletion. |
| Overall Complexity Score   | 65 (out of 100)                                                          |

---

### **6. Key Outputs**

*   **Regional Sales Analysis:** The primary output is the result of the query that groups the `sales` table by `region`. This provides a summary of `total_sales` and `total_amount` for each region, which is crucial for business performance monitoring.
*   **Frequent Customer List:** The `frequent_customers` view provides a list of customers who have made more than one purchase. This output can be used by marketing teams for targeted campaigns or loyalty programs.
*   **Data Manipulation Example:** The query using the `reverse_string` UDF serves as a technical output, demonstrating the capability to apply custom transformations within the Hive environment.

These outputs support downstream analytical systems, business intelligence dashboards, and data-driven marketing activities.

---

### **7. API Cost Calculations**

apiCost: 0.0015 USD