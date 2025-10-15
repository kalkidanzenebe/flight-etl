import pandas as pd
import yaml
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time

# Load environment variables from .env file
load_dotenv()

def load(df, table_name, chunk_size=10000):
    """
    Load data into PostgreSQL database
    
    Args:
        df (pandas.DataFrame): Transformed data to load
        table_name (str): Name of the table to load data into
        chunk_size (int): Number of rows to load at a time
    
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.empty:
        print("❌ No data to load")
        return False
    
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
        
        # Validate that all required environment variables are set
        missing_vars = []
        for var_name, var_value in [("DB_USER", db_user), ("DB_PASSWORD", db_pass), 
                                   ("DB_HOST", db_host), ("DB_PORT", db_port), ("DB_NAME", db_name)]:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            print(f"❌ Missing environment variables: {missing_vars}")
            print("Please ensure you have created a .env file with your database credentials")
            return False
        
        # Create database connection
        connection_string = f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        
        print(f"🔌 Connecting to PostgreSQL database '{db_name}'...")
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
        
        # Load data in chunks for large datasets
        total_rows = len(df)
        print(f"🚚 Loading {total_rows} rows into table '{table_name}' in chunks of {chunk_size}...")
        
        start_time = time.time()
        
        if total_rows <= chunk_size:
            # If data is small enough, load all at once
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"✅ Loaded {total_rows} rows successfully")
        else:
            # For large datasets, load in chunks
            chunks_loaded = 0
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                if_exists_param = 'replace' if i == 0 else 'append'
                chunk.to_sql(table_name, engine, if_exists=if_exists_param, index=False)
                chunks_loaded += 1
                
                # Progress update
                rows_loaded = min(i + chunk_size, total_rows)
                progress = (rows_loaded / total_rows) * 100
                print(f"   Progress: {rows_loaded}/{total_rows} rows ({progress:.1f}%)")
            
            print(f"✅ Loaded {total_rows} rows in {chunks_loaded} chunks successfully")
        
        end_time = time.time()
        load_time = end_time - start_time
        print(f"⏱️  Total load time: {load_time:.2f} seconds")
        
        # Verify data was loaded
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.fetchone()[0]
            print(f"🔍 Verified: {row_count} rows in table '{table_name}'")
        
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return False

if __name__ == "__main__":
    # This section can be used to test the load function independently
    print("💡 This script is designed to be imported and used by other scripts.")
    print("💡 To test the full ETL pipeline, run 'python scripts/etl_pipeline.py'")