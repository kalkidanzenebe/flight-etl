import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from transform import transform
from extract import extract

# Load environment variables from .env file
load_dotenv()

def load(df, table_name):
    try:
        # Load configuration
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        # Get database configuration from environment variables
        db_type = config["database"]["type"]
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        
        engine = create_engine(f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Data loaded into PostgreSQL table '{table_name}' successfully")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")

if __name__ == "__main__":
    # Build CSV path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "flights_sample_3m.csv")

    # Extract & Transform
    df = extract(csv_path)
    df = transform(df)

    # Load into PostgreSQL
    if df is not None:
        load(df, "flights")