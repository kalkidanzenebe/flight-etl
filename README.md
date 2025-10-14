# Flight ETL Core

This project implements an ETL (Extract, Transform, Load) pipeline for flight data.

## Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Set up your database credentials:
   - Copy `.env.example` to `.env`
   - Update the values in `.env` with your actual database credentials

3. Make sure your input data file is in the correct location as specified in `config/config.yaml`

## Running the ETL Pipeline

```
python scripts/etl_pipeline.py
```

## Configuration

The ETL pipeline uses two configuration mechanisms:

1. `config/config.yaml` - Contains non-sensitive configuration like file paths and ETL settings
2. Environment variables (`.env` file) - Contains sensitive information like database credentials

This approach ensures that sensitive data is not committed to the repository while maintaining a clear configuration structure.