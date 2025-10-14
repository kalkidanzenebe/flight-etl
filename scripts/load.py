import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from scripts.transform import transform
from scripts.extract import extract

# Load environment variables from .env file
load_dotenv()

def load(df, table_name):
    try:
        # Load configuration
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        # Get database configuration
        db_config = config["database"]
        # Use environment variables, fallback to config values if not set
        db_user = os.getenv("DB_USER", db_config["user"])
        db_pass = os.getenv("DB_PASSWORD", db_config["password"])
        db_host = os.getenv("DB_HOST", db_config["host"])
        db_port = os.getenv("DB_PORT", db_config["port"])
        db_name = os.getenv("DB_NAME", db_config["name"])
        
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
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