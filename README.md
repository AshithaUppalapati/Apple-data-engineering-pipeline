🍎 Apple Retail Data Engineering Pipeline
> An end-to-end Lakehouse pipeline built on **Apache Spark** and **Databricks** — ingesting raw retail transaction data, cleaning and enriching it through a Bronze → Silver → Gold architecture, and surfacing customer purchase-sequence insights using Delta Lake.
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=flat&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
---
📌 Business Problem
Apple retail teams need to understand cross-product purchase behavior to run effective upsell campaigns. This pipeline identifies customers who purchased AirPods immediately after buying an iPhone — a high-value signal for targeted marketing and inventory planning.
---
🏗️ Architecture Overview
The pipeline follows the industry-standard Medallion (Lakehouse) Architecture:
```plaintext
[Raw CSVs]
    │
    ▼
┌─────────────────────────────────────┐
│           BRONZE LAYER              │
│  Raw Delta tables — no transforms   │
│  Ingested from Unity Catalog Volumes│
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│           SILVER LAYER              │
│  • Product name normalization       │
│  • Customer + Product + Transaction │
│    3-way join                       │
│  • Analytics-ready unified table    │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│            GOLD LAYER               │
│  • Window-function sequence analysis│
│  • AirPods-after-iPhone detection   │
│  • Curated insight datasets         │
└─────────────────────────────────────┘
```
> Architecture diagrams available in [`architecture/`](./architecture/)
---
🛠️ Technologies Used
Tool	Purpose
Apache Spark (PySpark)	Distributed data processing and transformations
Databricks	Unified analytics platform and notebook execution
Delta Lake	ACID-compliant storage with time travel support
Unity Catalog Volumes	Managed storage for raw CSV ingestion
Python	Pipeline logic and modular source code
GitHub + Databricks Repos	Version control and CI/CD integration
---
📂 Dataset Description
Three input CSV files drive the pipeline:
`Customer_Updated.csv`
Customer profile data.
Column	Description
`customer_id`	Unique customer identifier (PK)
`customer_name`	Full name
`join_date`	Date customer joined
`location`	Geographic location
`Products_Updated.csv`
Product catalog with base product names.
Column	Description
`product_id`	Unique product identifier (PK)
`product_name`	Base product name (e.g. `iphone`, `airpods`)
`category`	Product category
`price`	Unit price
`Transaction_Updated.csv`
Purchase history with variant product names.
Column	Description
`transaction_id`	Unique transaction identifier (PK)
`customer_id`	Foreign key → customers
`product_name`	Raw variant name (e.g. `iphone se`, `airpods pro`)
`transaction_date`	Date of purchase
---
🔧 Key Transformations
1. Product Name Normalization (Silver Layer)
Transaction records contain product name variants that don't match the product catalog directly. A normalization step maps variants to base names using Spark `when()` conditions:
```python
from pyspark.sql.functions import when, col, lower

transactions_clean = transactions.withColumn(
    "product_name_normalized",
    when(lower(col("product_name")).contains("iphone"), "iphone")
    .when(lower(col("product_name")).contains("airpods"), "airpods")
    .when(lower(col("product_name")).contains("macbook"), "macbook")
    .when(lower(col("product_name")).contains("ipad"), "ipad")
    .when(lower(col("product_name")).contains("watch"), "apple watch")
    .otherwise(col("product_name"))
)
```
---
2. Three-Way Silver Join
After normalization, the pipeline joins all three datasets into a single enriched table:
```python
silver_df = (
    transactions_clean
    .join(customers, on="customer_id", how="inner")
    .join(products_clean, on=transactions_clean.product_name_normalized == products_clean.product_name, how="inner")
)
```
Silver output columns:
`customer_id`, `customer_name`, `location`, `product_id`, `product_name`, `category`, `price`, `transaction_date`
---
3. AirPods-After-iPhone Detection (Gold Layer)
Using Spark window functions to analyze sequential purchase patterns:
```python
from pyspark.sql.functions import lag
from pyspark.sql.window import Window

# Define window: partition by customer, ordered by date
window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")

# Add previous product column using lag()
gold_df = silver_df.withColumn(
    "prev_product",
    lag("product_name", 1).over(window_spec)
)

# Filter: AirPods purchased immediately after an iPhone
airpods_after_iphone = gold_df.filter(
    (col("product_name") == "airpods") &
    (col("prev_product") == "iphone")
)
```
---
📊 Output Schemas
Silver Layer Output
Column	Type	Description
`customer_id`	string	Customer identifier
`customer_name`	string	Full name
`location`	string	Geographic location
`product_id`	string	Product identifier
`product_name`	string	Normalized product name
`category`	string	Product category
`price`	double	Unit price
`transaction_date`	date	Date of purchase
Gold Layer Output — AirPods After iPhone
Column	Type	Description
`customer_id`	string	Customer who made the sequence
`customer_name`	string	Customer full name
`prev_product`	string	Previous purchase (`iphone`)
`product_name`	string	Current purchase (`airpods`)
`transaction_date`	date	Date of AirPods purchase
---
📁 Repository Structure
```
apple-data-engineering-pipeline/
│
├── notebooks/
│   └── apple_analysis_etl_project.ipynb   # Full ETL notebook
│
├── src/
│   ├── reader_factory.py                  # Configurable data reader
│   ├── transformer.py                     # Transformation logic
│   ├── loader_factory.py                  # Delta Lake writer
│   └── utils.py                           # Shared helpers
│
├── data_sample/
│   ├── Customer_Updated_sample.csv        # Sample customer data
│   ├── Products_Updated_sample.csv        # Sample product catalog
│   └── Transaction_Updated_sample.csv     # Sample transactions
│
├── architecture/
│   ├── lakehouse_architecture.png         # Medallion architecture diagram
│   └── pipeline_flow.png                  # End-to-end flow diagram
│
└── README.md
```
---
🚀 How to Run
Prerequisites
Databricks workspace with Unity Catalog enabled
Databricks Runtime 12.x or above (with Apache Spark 3.3+)
GitHub repo connected via Databricks Repos
Steps
1. Clone the repository into Databricks Repos
```
Databricks UI → Repos → Add Repo → Paste GitHub URL
```
2. Upload sample data to Unity Catalog Volumes
```
Upload files from data_sample/ to your designated Unity Catalog Volume path
```
3. Run the Bronze ingestion notebook
```python
# Reads raw CSVs from Volumes and writes Delta tables
# No transformations applied at this layer
```
4. Run the Silver transformation notebook
```python
# Normalizes product names
# Joins customers + products + transactions
# Writes unified Silver Delta table
```
5. Run the Gold analytics notebook
```python
# Applies window functions
# Identifies AirPods-after-iPhone purchase sequences
# Writes curated Gold Delta table
```
6. View results in Databricks
```python
display(airpods_after_iphone)
```
---
🎯 Skills Demonstrated
✅ End-to-end ETL pipeline design on Databricks
✅ Spark DataFrame transformations (filter, join, withColumn, when)
✅ Window functions (`lag()`) for purchase sequence analysis
✅ Delta Lake for ACID-compliant, versioned storage
✅ Medallion (Bronze → Silver → Gold) Lakehouse architecture
✅ Data cleaning and product name normalization strategies
✅ Unity Catalog Volumes for managed file ingestion
✅ Modular Python source code (`src/` layer separation)
✅ Databricks Repos + GitHub integration for version control
---
This project was built as part of a data engineering portfolio to demonstrate real-world Lakehouse pipeline skills using Apache Spark and Databricks.
