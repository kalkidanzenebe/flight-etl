# Flight ETL Core

This project implements an ETL (Extract, Transform, Load) pipeline for flight data, designed to meet the requirements of a data analytics assignment with at least 2 million records.

## Assignment Requirements Met

✅ **Data Source**: Uses a dataset with over 3 million flight records, exceeding the minimum requirement of 2 million records
✅ **ETL Process**: Utilizes Python's Pandas library for extraction, transformation, and loading
✅ **Database Management**: Stores transformed data in a PostgreSQL database with appropriate schema design

## Project Structure

```
fligh-etl-core/
├── config/                 # Configuration files
├── data/                   # Data files (ignored in git)
├── scripts/                # ETL pipeline scripts
├── .env.example           # Example environment variables
├── .gitignore             # Git ignore rules
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Up Database Credentials**:
   - Copy `.env.example` to `.env`
   - Update the values in `.env` with your actual PostgreSQL database credentials

3. **Data File**:
   The project uses a CSV file with flight data at `data/flights_sample_3m.csv`. 
   This file contains over 3 million records and is intentionally ignored by git 
   due to its large size (approximately 614MB). This approach follows best practices 
   for data science projects where large datasets are not committed to version control.

4. **For Assignment Submission**:
   Since the data file is not included in the repository, you should include instructions 
   for your instructor to either:
   - Obtain the dataset separately
   - Generate a sample dataset using the provided script
   - Use their own flight dataset with a similar schema

## Running the ETL Pipeline

```bash
python scripts/etl_pipeline.py
```

## Security Features

- Database credentials are stored in environment variables, not in the code
- Sensitive files are excluded from version control via `.gitignore`
- Configuration is separated from credentials for better security

## Business Scenario

This ETL pipeline processes flight data which can be used for:
- Airline performance analysis
- Route profitability studies
- Pricing strategy optimization
- Customer demand forecasting
- Operational efficiency improvements

## Schema Design

The PostgreSQL database schema is designed to efficiently store flight information including:
- Flight identifiers and airline information
- Origin and destination airports
- Departure and arrival times
- Flight duration and distance
- Pricing and availability data
- Flight status information

## Performance Considerations

Given the large dataset size (3 million+ records), the ETL pipeline is designed with performance in mind:
- Efficient data processing using Pandas
- Proper error handling
- Progress tracking during long-running operations