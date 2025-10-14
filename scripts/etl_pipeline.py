import yaml
import os
from dotenv import load_dotenv
from extract import extract
from transform import transform
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Extract
df = extract(config["paths"]["input_data"])

# Transform
df_clean = transform(df)

# Load
# Use environment variables for all database credentials
db_type = config["database"]["type"]
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f"{db_type}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

df_clean.to_sql('flights', engine, if_exists='replace', index=False)
print(" ETL pipeline completed: Data loaded into PostgreSQL!")