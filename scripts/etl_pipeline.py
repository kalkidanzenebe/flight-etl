import yaml
import os
from dotenv import load_dotenv
from scripts.extract import extract
from scripts.transform import transform
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
db_config = config["database"]
# Use environment variables, fallback to config values if not set
db_user = os.getenv("DB_USER", db_config["user"])
db_pass = os.getenv("DB_PASSWORD", db_config["password"])
db_host = os.getenv("DB_HOST", db_config["host"])
db_port = os.getenv("DB_PORT", db_config["port"])
db_name = os.getenv("DB_NAME", db_config["name"])

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

df_clean.to_sql('flights', engine, if_exists='replace', index=False)
print(" ETL pipeline completed: Data loaded into PostgreSQL!")