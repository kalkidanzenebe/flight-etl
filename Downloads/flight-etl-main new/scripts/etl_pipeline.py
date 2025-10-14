import yaml
import os
import time
from dotenv import load_dotenv
from extract import extract
from transform import transform
from load import load

# Load environment variables from .env file
load_dotenv()

def main():
    """
    Main ETL pipeline function
    """
    print("🚀 Starting Flight ETL Pipeline...")
    start_time = time.time()
    
    try:
        # Load configuration
        print("⚙️  Loading configuration...")
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        # Extract
        print("\n📥 EXTRACT PHASE")
        extract_start = time.time()
        df = extract(config["paths"]["input_data"], chunk_size=config["etl"]["chunk_size"])
        extract_end = time.time()
        print(f"⏱️  Extract time: {extract_end - extract_start:.2f} seconds")
        
        if df is None:
            print("❌ Extract phase failed. Exiting pipeline.")
            return False
        
        # Transform
        print("\n🔄 TRANSFORM PHASE")
        transform_start = time.time()
        df_clean = transform(df)
        transform_end = time.time()
        print(f"⏱️  Transform time: {transform_end - transform_start:.2f} seconds")
        
        if df_clean is None:
            print("❌ Transform phase failed. Exiting pipeline.")
            return False
        
        # Load
        print("\n📤 LOAD PHASE")
        load_start = time.time()
        success = load(df_clean, 'flights', chunk_size=config["etl"]["chunk_size"])
        load_end = time.time()
        print(f"⏱️  Load time: {load_end - load_start:.2f} seconds")
        
        if not success:
            print("❌ Load phase failed.")
            return False
        
        # Pipeline completion
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n🎉 ETL Pipeline Completed Successfully!")
        print(f"📊 Processed {len(df_clean)} rows")
        print(f"⏱️  Total execution time: {total_time:.2f} seconds")
        
        return True
        
    except Exception as e:
        print(f"❌ ETL Pipeline failed with error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ ETL pipeline finished successfully!")
    else:
        print("\n❌ ETL pipeline failed!")
        exit(1)