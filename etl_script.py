import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
import openpyxl

# Extract data 
def extract_data(file_path):
    try:
        print("Extracting data from Excel file...")
        df = pd.read_excel(file_path)  # Read Excel into a DataFrame
        print(f"Extracted {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

# Clean and Transform data
def clean_data(df):
    try:
        print("Cleaning data...")
        df.dropna(inplace=True)  # Remove null values
        df.drop_duplicates(inplace=True)  # Remove duplicate rows
        print(f"Data after dropping nulls and duplicates: {len(df)} records.")

        # Convert signup_date to standard YYYY-MM-DD format
        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        str_cols = ['name', 'gender', 'email', 'address', 'country', 'department', 'designation']

        # Strip leading/trailing whitespace
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

        # Convert relevant fields to lowercase
        df[['email', 'address', 'designation', 'country']] = df[['email', 'address', 'designation', 'country']].apply(lambda x: x.str.lower())

        # Ensure consistency in country values (usa, uk, india)
        df = df[df['country'].isin(['usa', 'uk', 'india'])]
        print(f"Filtered records by country (usa, uk, india): {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error cleaning data: {e}")
        return pd.DataFrame()

# Save the cleaned data into separate CSV
def save_by_country(df, output_dir='final_data'):
    try:
        print(f"Saving cleaned data by country to {output_dir}/ directory...")
        os.makedirs(output_dir, exist_ok=True)  
        for country in ['usa', 'uk', 'india']:
            df_country = df[df['country'] == country]  
            path = os.path.join(output_dir, f'country_{country}.csv')
            df_country.to_csv(path, index=False)  
            print(f"Saved {len(df_country)} records to {path}")
    except Exception as e:
        print(f"Error saving country CSVs: {e}")

# Load USA customer data into MySQL database
def load_to_mysql(df):
    try:
        print("Loading data into MySQL database...")
        # Use SQLAlchemy engine to connect to MySQL
        engine = create_engine('mysql+mysqlconnector://Zaid:22155@127.0.0.1/customer_us_db')
        conn = engine.connect()
        conn.execute("""
        CREATE TABLE IF NOT EXISTS us_customers (
                name varchar(64), 
                gender varchar(64), 
                country varchar(64), 
                department varchar(64),
                designation varchar(64),
                email varchar(64),
                signup_date DATE,
                address varchar(255));
        """)
        for row in df.itertuples():
            conn.execute(f"INSERT INTO us_customers (name, gender, country, department, designation, email, signup_date, address) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (row.name, row.gender, row.country, row.department, row.designation, row.email, row.signup_date, row.address))
        print("Loaded data into MySQL table 'us_customers'.")
    except Exception as e:
        print(f"MySQL load error: {e}")

# Load UK and India customer data into PostgreSQL database
def load_to_postgresql(df):
    try:
        print("Loading data into PostgreSQL database...")
        # Use SQLAlchemy engine to connect to PostgreSQL
        engine = psycopg2.connect(
            dbname="customer_global_db",
            user="postgres",
            password="myfirstDE@rcai",
            host="localhost",
            port="5432")
        cursor = engine.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS global_customers (
                name varchar(64), 
                gender varchar(64), 
                country varchar(64), 
                department varchar(64),
                designation varchar(64),
                email varchar(64),
                signup_date DATE,
                address varchar(255));
        """)
        for row in df.itertuples():
            cursor.execute(f"INSERT INTO global_customers (name, gender, country, department, designation, email, signup_date, address) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (row.name, row.gender, row.country, row.department, row.designation, row.email, row.signup_date, row.address))
        engine.commit()
        cursor.close()
        engine.close()
        print("Loaded data into PostgreSQL table 'global_customers'.")
    except Exception as e:
        print(f"PostgreSQL load error: {e}")    

# Generate metadata
def generate_metadata(df, output_file='metadata.json'):
    try:
        print("Generating metadata...")
        metadata = {
            "records_total": len(df),  
            "records_by_country": df['country'].value_counts().to_dict(),  
            "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            "column_info": {
                col: {
                    "dtype": str(df[col].dtype),  
                    "sample": df[col].dropna().unique()[:3].tolist() 
                } for col in df.columns
            }
        }
        with open(output_file, 'w') as f:
            json.dump(metadata, f, indent=4)  # Save metadata to JSON
        print(f"Metadata saved to {output_file}")
    except Exception as e:
        print(f"Error generating metadata: {e}")

# Main function to orchestrate the ETL pipeline
def main():
    print("Starting ETL process...")
    df = extract_data('messy_customers_data.xlsx')  # Extract
    if df.empty:
        print("Extraction failed or returned empty data. Exiting.")
        return

    df_clean = clean_data(df)  # Transform
    if df_clean.empty:
        print("Cleaning failed or resulted in empty data. Exiting.")
        return

    save_by_country(df_clean)  # Save cleaned data

    # Load data into databases
    load_to_mysql(df_clean[df_clean['country'] == 'usa'])
    load_to_postgresql(df_clean[df_clean['country'].isin(['uk', 'india'])])

    generate_metadata(df_clean)  # Generate metadata
    print("ETL process completed successfully.")

# Run the main function
if __name__ == '__main__':
    main()