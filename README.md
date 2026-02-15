# PySpark + DBT Data Pipeline

A modern data pipeline built on **Databricks** that combines **PySpark** and **dbt** for scalable data processing and transformation.

## Architecture

```
Source Data → PySpark (Bronze) → PySpark/dbt (Silver) → dbt (Gold)
```

### Pipeline Layers

- **Bronze Layer**: Raw data ingestion using PySpark Structured Streaming with Delta Lake
- **Silver Layer**: Data cleansing and transformation using PySpark notebooks and dbt incremental models  
- **Gold Layer**: Business-ready dimensional models with SCD Type 2 using dbt snapshots

## Tech Stack

- **PySpark**: Structured streaming, data transformations, and CDC operations
- **dbt**: Incremental models, SCD snapshots, and SQL transformations
- **Delta Lake**: ACID transactions and versioning on Databricks
- **Unity Catalog**: Direct table creation in Databricks catalog

## Key Features

### PySpark Implementation
- Dynamic streaming ingestion for multiple entities (customers, trips, locations, payments, drivers, vehicles)
- Custom utility classes for deduplication, upsert operations, and timestamp processing
- CDC-based merge logic with Delta Lake

### dbt Implementation  
- Incremental models with smart timestamp-based loading
- SCD Type 2 snapshots for dimensional tables
- Custom schema generation macros
- Source definitions connecting Bronze and Silver layers

## Project Structure

```
├── pyspark_notebooks/
│   ├── bronze_ingestion.ipynb      # Streaming ingestion to Bronze
│   ├── silver_transformation.ipynb  # PySpark transformations to Silver
│   └── utils/custom_utils.py        # Reusable transformation classes
├── models/
│   ├── silver/trips.sql             # dbt incremental model
│   └── sources/sources.yml          # Source configurations
├── snapshots/
│   ├── scd.yml                      # SCD Type 2 dimensions
│   └── fact.yml                     # Fact table snapshots
└── dbt_project.yml                  # dbt configurations
```

## Pipeline Workflow

1. **Ingestion**: PySpark reads CSV files and streams data to Bronze Delta tables in Unity Catalog
2. **Transformation**: PySpark notebooks apply business logic, deduplication, and write to Silver layer
3. **Modeling**: dbt processes Silver tables into incremental models and Gold dimensional tables
4. **History**: dbt snapshots maintain historical changes with SCD Type 2 strategy

## Running the Pipeline

### Bronze Layer (PySpark)
```python
# Run bronze_ingestion.ipynb in Databricks
# Creates tables: pyspark_proj.bronze.{entity}
```

### Silver Layer (PySpark + dbt)
```python
# Run silver_transformation.ipynb for PySpark transformations
```

```bash
# Run dbt models
dbt run --models silver
```

### Gold Layer (dbt)
```bash
# Create/update SCD dimensions
dbt snapshot
```

## Configuration

The pipeline uses:
- **Database**: `pyspark_proj`
- **Schemas**: `bronze`, `silver`, `gold`
- **Volume Path**: `/Volumes/pyspark_proj/source/source_data/`
- **Checkpoint Location**: `/Volumes/pyspark_proj/bronze/checkpoint/`

---

Built with ❤️ using PySpark and dbt on Databricks
