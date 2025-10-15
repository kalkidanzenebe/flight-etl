
from importlib.metadata import files
import os
import pandas as pd
import yaml

def extract(file_path, chunk_size=None):
    """
    Extract data from CSV file
    
    Args:
        file_path (str): Path to the CSV file
        chunk_size (int, optional): Number of rows to read at a time for large files
    
    Returns:
        pandas.DataFrame or None: Extracted data or None if failed
    """
    try:

        if not os.path.exists(file_path):
            print(f" File not found at: {file_path}")
            return None
        
       
        file_size = os.path.getsize(file_path) / (1024 * 1024)  
        print(f" Loading data from {file_path} ({file_size:.2f} MB)...")
        
        
        if chunk_size:
            print(f"🔄 Reading data in chunks of {chunk_size} rows...")
            chunks = pd.read_csv(file_path, chunksize=chunk_size)
            df_list = []
            total_rows = 0
            
            for i, chunk in enumerate(chunks):
                df_list.append(chunk)
                total_rows += len(chunk)
                if (i + 1) % 10 == 0:  
                    print(f"   Processed {total_rows} rows...")
            
            df = pd.concat(df_list, ignore_index=True)
            print(f"✅ Extracted {len(df)} rows from {file_path} (chunked reading)")
        else:
          
            df = pd.read_csv(file_path)
            print(f"✅ Extracted {len(df)} rows from {file_path}")
        
       
        print(f"📊 Data shape: {df.shape}")
        print(f"📋 Columns: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f" Failed to extract data from {file_path}: {e}")
        return None

if name == "main":
    
    try:
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        csv_path = config["paths"]["input_data"]
        extract(csv_path, chunk_size=10000)  
    except Exception as e:
        print(f" Error loading configuration: {e}")
       
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(files)))
        csv_path = os.path.join(base_dir, "data", "flights_sample_3m.csv")
        extract(csv_path, chunk_size=10000)