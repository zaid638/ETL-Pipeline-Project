# ETL Pipeline: Excel to MySQL & PostgreSQL

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python, SQL and Excel. It reads raw customer data, cleans and standardizes it, and loads the cleaned data into MySQL and PostgreSQL databases. It also generates metadata about the processed data.

---

![ETL image](https://github.com/zaid638/Marketing-ETL-Pipeline/blob/main/Marketing_ETL_Diagram.png)

---

## 🧩 Features

- ✅ Extracts data from an Excel file
- ✅ Cleans and transforms the data (dates, strings, countries)
- ✅ Splits and saves data by country into CSVs
- ✅ Loads data into:
  - MySQL (`customer_us_db`) for USA customers
  - PostgreSQL (`customer_global_db`) for UK and India customers
- ✅ Generates metadata JSON report

---


## 📁 Directory Structure

DataEngineering_Assesment/
│
├── README.md
├── messy_customers_data.xlsx
├── etl_script.py
├── metadata.json
└── final_data/
    ├── country_usa.csv
    ├── country_uk.csv
    └── country_india.csv

## 🚀 Getting Started

### Prerequisites


- Python 3.7+
- MySQL and PostgreSQL running locally
- Required Python libraries:


````pip install pandas openpyxl sqlalchemy mysql-connector-python psycopg2````


### Database Setup

MySQL:

      CREATE DATABASE customer_us_db;

PostgreSQL:

      CREATE DATABASE customer_global_db;

Configuration:

Edit the database connection URLs in etl_script.py

      mysql+mysqlconnector://user:password@localhost/customer_us_db
      postgresql+psycopg2://user:password@localhost/customer_global_db

Replace ````user```` and ````password```` with your actual DB credentials.

▶️ Run the ETL Process

````etl_script.py````
