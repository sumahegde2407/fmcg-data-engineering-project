# FMCG Data Engineering Project

## Business Problem
A sports equipment parent company acquired a nutrition/energy bar subsidiary. Built an end-to-end data pipeline to
consolidate data from both companies into a unified Gold layer for business analytics and reporting.

## Architecture

```
ONE TIME (Full Load):
Parent data → directly imported into Databricks Gold
Child data → ADLS → Bronze → Silver → Gold → Merge into Parent Gold

DAILY (Incremental Load):
New orders CSV uploaded to ADLS Gen2
        ↓
ADF Storage Event Trigger fires automatically
        ↓
ADF Copy Activity moves file to processed zone
        ↓
Databricks Workflow triggered (4 tasks):
  Task 1: customers_data_processing
  Task 2: products_data_processing
  Task 3: pricing_data_processing
  Task 4: incremental_load_fact
        ↓
Bronze → Silver → Gold (child company)
        ↓
Merge Child Gold + Parent Gold → Unified fact_orders table
```

## Tech Stack
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Data Factory (ADF)
- Azure Databricks (Free Edition)
- PySpark
- Delta Lake
- Databricks Workflows

## Key Features
- Event-driven pipeline (Storage Event Trigger)
- Full load + Incremental load (watermark-based)
- Medallion Architecture (Bronze → Silver → Gold)
- Multi-source data consolidation (Parent OLTP + Child S3/ADLS)
- Star Schema data modeling (fact + dimension tables)
- Delta MERGE for idempotent upserts
- Data deduplication before MERGE
- Parameterized notebooks using dbutils.widgets

## Data Model
- fact_orders — unified parent + child transactions
- dim_customers — customer dimension
- dim_products — product dimension
- dim_date — date dimension
- dim_gross_price — pricing dimension

## Dataset
- Parent company: Sports equipment (established Gold tables)
- Child company: Energy bars/nutrition (raw CSVs in ADLS)
- Combined: 96,684 records in unified Gold table
- 406 unique products, 54 unique customers
- Date range: Jan 2024 – Dec 2025

## How to Run
1. Upload raw CSV to `source/child/orders/landing/` in ADLS
2. ADF pipeline triggers automatically
3. Databricks Workflow processes Bronze → Silver → Gold
4. Query unified Gold table: `databricksmaster.gold.fact_orders`


