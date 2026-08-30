# Data Warehouse ETL Pipeline

## 📌 Project Overview

This project implements an end-to-end Data Warehouse ETL pipeline using Python, Pandas, and PostgreSQL.

The pipeline extracts customer, product, and order data from CSV files, transforms the data into a warehouse-friendly structure, and loads it into a PostgreSQL Data Warehouse using a Star Schema.

The project also implements Slowly Changing Dimension (SCD) Type 2 to maintain historical customer information.

---

## 🏗️ Architecture

```text
CSV Files
   │
   ▼
Extract
   │
   ▼
Transform
   │
   ├───────────────┐
   ▼               ▼
Customer SCD2    Products
   │               │
   ▼               ▼
dim_customer    dim_product
       │             │
       └──────┬──────┘
              ▼
          fact_sales
              ▲
              │
          dim_date
              │
              ▼
       Analytical Queries