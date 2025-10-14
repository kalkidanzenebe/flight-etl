import pandas as pd

def transform(df):
    if df is None:
        print("❌ No data to transform")
        return None

    
    df = df.dropna(subset=['FL_DATE', 'AIRLINE'])

   
    df['FL_DATE'] = pd.to_datetime(df['FL_DATE'], errors='coerce')
    df = df.dropna(subset=['FL_DATE'])  

  
    delay_cols = [
        'DEPARTURE_DELAY', 'ARRIVAL_DELAY', 'DELAY_DUE_WEATHER', 
        'DELAY_DUE_NAS', 'DELAY_DUE_SECURITY', 'DELAY_DUE_LATE_AIRCRAFT'
    ]
    for col in delay_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    print("✅ Transformation complete")
    return df

if name == "main":
    from extract import extract
    import os

   
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(file)))
    csv_path = os.path.join(base_dir, "data", "dataset.csv")

    df = extract(csv_path)
    df = transform(df)
    print(df.head())