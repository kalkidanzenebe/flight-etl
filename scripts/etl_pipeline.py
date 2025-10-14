import yaml
from scripts.extract import extract
from scripts.transform import transform
from sqlalchemy import create_engine

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Extract
df = extract(config["paths"]["input_data"])

# Transform
df_clean = transform(df)

# Load
db_config = config["database"]
engine = create_engine(f"{db_config['type']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}")

df_clean.to_sql('flights', engine, if_exists='replace', index=False)
print(" ETL pipeline completed: Data loaded into PostgreSQL!")