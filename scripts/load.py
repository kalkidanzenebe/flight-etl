import pandas as pd
from sqlalchemy import create_engine
from transform import transform
from extract import extract
import os

# PostgreSQL connection info
DB_USER = "postgres"
DB_PASSWORD = "2552"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "flightdb"

def load(df, table_name):
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Data loaded into PostgreSQL table '{table_name}' successfully")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")

if name == "main":
    # Build CSV path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(file)))
    csv_path = os.path.join(base_dir, "data", "dataset.csv")

    # Extract & Transform
    df = extract(csv_path)
    df = transform(df)

    # Load into PostgreSQL
    if df is not None:
        load(df, "flights")
 load code