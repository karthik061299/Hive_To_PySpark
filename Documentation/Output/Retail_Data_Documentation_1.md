=============================================
Author:        Ascendion AVA+
Created on:   
Description:   This document outlines a Hive workflow for processing and analyzing retail data, including customer orders, sales, and transactions.
=============================================

---

### **1. Overview of Job**

*   The Hive job processes retail data by creating several tables to store customer information, orders, sales, and transactions. It demonstrates key Hive features such as table creation, joins, partitioning (static and dynamic), bucketing, views, and the use of a temporary User-Defined Function (UDF).
*   The job aligns with standard data warehousing practices by structuring raw data into meaningful, queryable formats for analysis.
*   The business process supported is the analysis of sales performance by region and the identification of frequent customers.
*   The workflow includes external tables (`customers`, `orders`), managed tables (`sales`, `transactions`), a join between customers and orders, dynamic partitioning to load the `sales` table, aggregations to analyze sales, and a view to find frequent customers.

---

### **2. Job Structure and Design**

*   The script follows a sequential structure designed for a complete, self-contained demonstration.
*   **Major logical steps:**
    1.  **Setup:** Creates a database `retail_db` to encapsulate all objects.
    2.  **Table Creation:** Defines external tables (`customers`, `orders`) and managed tables (`sales`, `transactions`). The `sales` table is partitioned, and the `transactions` table is bucketed.
    3.  **Data Transformation & Loading:** A `CASE` statement within an `INSERT` query handles the logic for dynamic partitioning, loading data from the `transactions` table into the appropriate partition in the `sales` table.
    4.  **Analysis:** Aggregations (`COUNT`, `SUM`) are used to analyze sales data. A view (`frequent_customers`) is created using a join, filter (`HAVING`), and aggregation to identify repeat customers.
    5.  **Cleanup:** Drops the view, all tables, and the database to ensure a clean state after execution.
*   **Reusable Components:** A temporary UDF (`reverse_string`) is created for a custom transformation, demonstrating extensibility.
*   **Dependencies:** The script implicitly depends on external comma-delimited text files for the `customers` and `orders` tables.

---

### **3. Data Flow and Processing Logic**

```
+----------------------------------------------+
| [Start]                                      |
| Description: Create retail_db database.      |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Create Tables]                              |
| Description: Create customers, orders,       |
| sales, and transactions tables.              |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Join Customers and Orders]                  |
| Description: Join tables to link customers   |
| with their orders.                           |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Load Sales Data]                            |
| Description: Insert data from transactions   |
| into sales using dynamic partitioning.       |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Analyze Sales]                              |
| Description: Aggregate sales data by region. |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Create Frequent Customers View]             |
| Description: Identify customers with more    |
| than one order.                              |
+----------------------------------------------+
↓
+----------------------------------------------+
| [Cleanup]                                    |
| Description: Drop all created objects.       |
+----------------------------------------------+
↓
+----------------------------------------------+
| [End]                                        |
| Description: Workflow complete.              |
+----------------------------------------------+
```

---

### **4. Data Mapping**

| Target Table Name  | Target Column Name | Source Table/Step Name | Source Column Name | Transformation Rule / Business Logic                                  |
| ------------------ | ------------------ | ---------------------- | ------------------ | --------------------------------------------------------------------- |
| sales              | sale_id            | transactions           | transaction_id     | Direct mapping                                                        |
| sales              | sale_amount        | transactions           | amount             | Direct mapping                                                        |
| sales              | sale_date          | transactions           | transaction_date   | Direct mapping                                                        |
| sales              | region             | transactions           | customer_id        | `CASE WHEN customer_id IN (1, 2) THEN 'north_america' ELSE 'europe' END` |
| frequent_customers | customer_name      | customers              | customer_name      | Group by customer_name                                                |
| frequent_customers | total_orders       | orders                 | order_id           | `COUNT(o.order_id)`                                                   |

---

### **5. Complexity Analysis**

| Category                   | Measurement                                           |
| -------------------------- | ----------------------------------------------------- |
| Number of Tables Used      | 4 (customers, orders, sales, transactions) + 1 View   |
| Source/Target Systems      | HDFS (for Textfile storage)                           |
| Transformation Steps       | 5 (Join, Dynamic Partition Insert, Aggregation, UDF, View Creation) |
| Parameters Used            | 2 (`hive.exec.dynamic.partition`, `hive.exec.dynamic.partition.mode`) |
| Reusable Components        | 1 (Temporary UDF `reverse_string`)                    |
| Control Logic              | 1 (CASE statement for partitioning)                   |
| External Dependencies      | 2 (Input data files for `customers` and `orders`)     |
| Performance Considerations | Partitioning (`sales`), Bucketing (`transactions`)    |
| Volume Handling            | Dynamic partitioning is suitable for large datasets.  |
| Error Handling             | None explicitly defined.                              |
| Overall Complexity Score   | 65 (out of 100)                                       |

---

### **6. Key Outputs**

*   The primary outputs of this script are the results of the analytical queries, which are displayed to the console upon execution. The script does not conclude with a persistent output table, as it includes a cleanup phase that drops all created objects.
*   **Key analytical insights generated:**
    *   A summary of total sales counts and total sales amounts, aggregated by region.
    *   A list of frequent customers, defined as those who have placed more than one order.
*   To make these outputs persistent, the final `SELECT` statements would need to be modified to write to a new Hive table or an external file system location.

---

### **7. API Cost Calculations**

*   `apiCost: 0.0015 USD`
