# Data Warehouse ETL Pipeline

A production-style batch ETL pipeline built with **Python, Pandas and PostgreSQL**.

The project extracts customer, product and order data from CSV files, transforms the data, applies Slowly Changing Dimension Type 2 (SCD2) logic to customers, and loads the processed data into a dimensional data warehouse.

## Architecture

```text
                    CSV SOURCE DATA
                         │
          ┌──────────────┼──────────────┐
          │              │              │
      customers.csv  products.csv   orders.csv
          │              │              │
          └──────────────┼──────────────┘
                         ↓
                     EXTRACT
                         ↓
                    TRANSFORM
              ┌──────────┼──────────┐
              │          │          │
          Customers   Products    Orders
              │          │          │
           SCD Type 2    │          │
              │          │          │
              └──────────┼──────────┘
                         ↓
                       LOAD
                         ↓
              ┌─────────────────────┐
              │   PostgreSQL DW     │
              │                     │
              │ dim_customer        │
              │ dim_product         │
              │ dim_date            │
              │ fact_sales          │
              └─────────────────────┘
                         ↓
                 Analytical Queries
```

## Tech Stack

* Python
* Pandas
* PostgreSQL
* psycopg
* SQL
* pytest
* python-dotenv

## Key Features

### ETL Pipeline

* CSV-based data extraction
* Data transformation using Pandas
* Data validation
* Duplicate removal
* Data type conversion
* Fact and dimension creation

### Data Warehouse

Uses a dimensional model containing:

* `dim_customer`
* `dim_product`
* `dim_date`
* `fact_sales`

### Slowly Changing Dimension Type 2

Customer changes are tracked historically.

For example:

```text
Customer 101

Version 1
Mumbai
is_current = FALSE

        ↓ city changes

Version 2
Nashik
is_current = TRUE
```

This preserves the customer's historical state.

### Incremental Processing

The pipeline supports incremental order processing using a PostgreSQL metadata table.

```text
pipeline_metadata
        ↓
last_processed_date
        ↓
lookback window
        ↓
incremental orders
```

A one-day lookback is used to reduce the risk of missing late-arriving records.

### UPSERT

Fact records use PostgreSQL UPSERT logic.

Existing orders are updated while new orders are inserted.

### Transaction Handling

Database operations use transactions with rollback handling.

If a load operation fails:

```text
Load
 ↓
Error
 ↓
ROLLBACK
 ↓
No partial transaction
```

### Data Quality

The pipeline performs checks for:

* Empty warehouse tables
* Duplicate order IDs
* NULL values
* Invalid quantities
* Negative sales amounts
* Invalid customer keys
* Invalid product keys
* Invalid date keys

### Testing

Transformation logic is tested using `pytest`.

Run:

```bash
python -m pytest
```

Expected result:

```text
4 passed
```

## Project Structure

```text
data-warehouse-etl/
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
│
├── tests/
│   ├── __init__.py
│   └── test_transform.py
│
├── extract.py
├── transform.py
├── load.py
├── main.py
├── config.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd data-warehouse-etl
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file based on `.env.example`.

Example:

```text
DATABASE_HOST=localhost
DATABASE_PORT=5433
DATABASE_NAME=your_database
DATABASE_USER=your_user
DATABASE_PASSWORD=your_password
```

### 5. Run the ETL pipeline

```bash
python main.py
```

### 6. Run tests

```bash
python -m pytest
```

## Pipeline Flow

```text
Extract
   ↓
Transform
   ↓
Customer SCD2
   ↓
Dimension Loading
   ↓
Fact Loading
   ↓
Data Quality
   ↓
Incremental Watermark
```

## Future Improvements

* Cloud storage integration
* Apache Spark / PySpark processing
* Azure Data Lake integration
* CI/CD pipeline
* Advanced monitoring
* Streaming ingestion with Kafka

## Author

Built as part of a hands-on Data Engineering learning journey.
