import os
import pandas as pd

def extract(file_path):
    if not os.path.exists(file_path):
        print(f"File not found at: {file_path}")
        return None
    df = pd.read_csv(file_path)
    print(f"Extracted {len(df)} rows from {file_path}")
    print(df.head())
    return df

if os.name == "main":
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(file)))
    csv_path = os.path.join(base_dir, "data", "dataset.csv")
    extract(csv_path) 
extract