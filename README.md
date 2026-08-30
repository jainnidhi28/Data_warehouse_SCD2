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

ETL Pipeline
1. Extract

Data is extracted from three CSV files:

customers.csv
products.csv
orders.csv

Python Pandas is used to read the files.

2. Transform

The transformation layer performs:

Data cleaning
String standardization
Duplicate removal
Data validation
Date conversion
Numeric conversion
Surrogate key generation
Date dimension creation
Fact table preparation

3. Load

The transformed data is loaded into PostgreSQL.
The warehouse contains:

dim_customer
dim_product
dim_date
fact_sales

Star Schema

The warehouse follows a Star Schema design.

              dim_customer
                    │
                    │
dim_product ─── fact_sales ─── dim_date
Fact Table
fact_sales

Contains measurable business events.

Columns:

sales_key
order_id
customer_key
product_key
date_key
quantity
sales_amount
Dimension Tables
dim_customer

Contains customer information.

Important columns:

customer_key
customer_id
customer_name
city
age
start_date
end_date
is_current
dim_product

Contains product information.

Columns:

product_key
product_id
product_name
category
price
dim_date

Contains calendar information.

Columns:

date_key
full_date
day
month
quarter
year

Slowly Changing Dimension Type 2

The customer dimension uses SCD Type 2 to preserve historical changes.

For example, if Rahul moves from Mumbai to Nashik:

customer_key | customer_id | city   | is_current
-------------|-------------|--------|------------
1            | 101         | Mumbai | false
7            | 101         | Nashik | true

The old record is not overwritten.

Instead:

The old record is closed.
end_date is populated.
is_current becomes false.
A new surrogate key is generated.
A new customer version is inserted.
The new record becomes current.

This allows historical reporting.