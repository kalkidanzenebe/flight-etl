import pandas as pd
import numpy as np

def transform(df):
    """
    Transform flight data
    
    Args:
        df (pandas.DataFrame): Raw flight data
    
    Returns:
        pandas.DataFrame: Transformed flight data or None if failed
    """
    if df is None:
        print("❌ No data to transform")
        return None
    
    print("🔄 Starting data transformation...")
    original_rows = len(df)
    
    # Display initial info
    print(f"📊 Initial data shape: {df.shape}")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Remove rows with completely missing data
    df = df.dropna(how='all')
    print(f"🧹 Removed {original_rows - len(df)} completely empty rows")
    
    # Handle the actual columns in the dataset
    # Convert datetime columns - FL_DATE is the date column
    if 'FL_DATE' in df.columns:
        df['FL_DATE'] = pd.to_datetime(df['FL_DATE'], errors='coerce')
    
    # Convert numeric columns
    numeric_cols = [
        'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 
        'WHEELS_ON', 'TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 
        'CRS_ELAPSED_TIME', 'ELAPSED_TIME', 'AIR_TIME', 'DISTANCE',
        'DELAY_DUE_CARRIER', 'DELAY_DUE_WEATHER', 'DELAY_DUE_NAS', 
        'DELAY_DUE_SECURITY', 'DELAY_DUE_LATE_AIRCRAFT'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows where critical information is missing
    # Using the actual column names from the dataset
    critical_cols = ['FL_DATE', 'AIRLINE', 'ORIGIN', 'DEST']
    df = df.dropna(subset=critical_cols)
    
    # Remove rows with invalid airport codes (should be 3 characters)
    if 'ORIGIN' in df.columns:
        df = df[df['ORIGIN'].str.len() == 3]
    if 'DEST' in df.columns:
        df = df[df['DEST'].str.len() == 3]
    
    # Ensure origin and destination are different
    if 'ORIGIN' in df.columns and 'DEST' in df.columns:
        df = df[df['ORIGIN'] != df['DEST']]
    
    # Clean up cancellation and diversion flags
    if 'CANCELLED' in df.columns:
        df['CANCELLED'] = df['CANCELLED'].fillna(0)
    if 'DIVERTED' in df.columns:
        df['DIVERTED'] = df['DIVERTED'].fillna(0)
    
    # Round numeric values for consistency
    for col in ['DEP_DELAY', 'ARR_DELAY', 'DISTANCE']:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    # Add derived columns
    if 'FL_DATE' in df.columns:
        df['year'] = df['FL_DATE'].dt.year
        df['month'] = df['FL_DATE'].dt.month
        df['day_of_week'] = df['FL_DATE'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
    
    print(f"✅ Transformation complete!")
    print(f"📊 Final data shape: {df.shape}")
    print(f"📉 Rows removed during transformation: {original_rows - len(df)}")
    
    # Display sample of transformed data
    print("\n📋 Sample of transformed data:")
    print(df.head())
    
    return df

if __name__ == "__main__":
    from extract import extract
    import os
    
    # Load data using the configured path
    try:
        import yaml
        with open("config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        csv_path = config["paths"]["input_data"]
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        # Fallback to default path
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, "data", "flights_sample_3m.csv")
    
    # Extract and transform
    df = extract(csv_path, chunk_size=10000)  # Use chunking for large files
    df = transform(df)
    
    if df is not None:
        print("\n🎉 Transform script completed successfully!")
    else:
        print("\n❌ Transform script failed!")