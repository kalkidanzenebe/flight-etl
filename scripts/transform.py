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
    
    # Handle specific columns based on the generated flight data schema
    # Convert datetime columns
    datetime_cols = ['departure_time', 'arrival_time']
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert numeric columns
    numeric_cols = ['flight_duration', 'distance', 'price', 'seats_available']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Ensure positive values for numeric columns
    for col in ['flight_duration', 'distance', 'price']:
        if col in df.columns:
            df[col] = np.abs(df[col])
    
    # Remove rows where critical information is missing
    critical_cols = ['flight_id', 'airline', 'origin_airport', 'destination_airport']
    df = df.dropna(subset=critical_cols)
    
    # Remove rows with invalid airport codes (should be 3 characters)
    if 'origin_airport' in df.columns:
        df = df[df['origin_airport'].str.len() == 3]
    if 'destination_airport' in df.columns:
        df = df[df['destination_airport'].str.len() == 3]
    
    # Ensure origin and destination are different
    if 'origin_airport' in df.columns and 'destination_airport' in df.columns:
        df = df[df['origin_airport'] != df['destination_airport']]
    
    # Clean up status values
    if 'status' in df.columns:
        valid_statuses = ['On Time', 'Delayed', 'Cancelled', 'Diverted']
        df['status'] = df['status'].apply(lambda x: x if x in valid_statuses else 'Unknown')
    
    # Round numeric values for consistency
    for col in ['flight_duration', 'distance', 'price']:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    # Add derived columns if not already present
    if 'departure_time' in df.columns and 'year' not in df.columns:
        df['year'] = df['departure_time'].dt.year
    if 'departure_time' in df.columns and 'month' not in df.columns:
        df['month'] = df['departure_time'].dt.month
    if 'departure_time' in df.columns and 'day_of_week' not in df.columns:
        df['day_of_week'] = df['departure_time'].dt.dayofweek
    if 'day_of_week' in df.columns and 'is_weekend' not in df.columns:
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