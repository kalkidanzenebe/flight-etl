from scripts.extract import extract
from scripts.transform import transform
from sqlalchemy import create_engine

# Extract
df = extract("data/dataset.csv")

# Transform
df_clean = transform(df)

# Load
db_user = "your_username"
db_pass = "your_password"
db_host = "localhost"
db_port = "5432"
db_name = "flightdb"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

df_clean.to_sql('flights', engine, if_exists='replace', index=False)
print(" ETL pipeline completed: Data loaded into PostgreSQL!")