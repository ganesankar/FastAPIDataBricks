# FastAPI S3 CSV Reader

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS Credentials:
- Copy `.env.example` to `.env`
- Fill in your AWS credentials and S3 bucket details

## Running the Application

```bash
uvicorn src.main:app --reload
```

## API Endpoint

`GET /read_csv?file_path=path/to/your/file.csv&target_table=workspace.default.customers`

Parameters:
- `file_path`: S3 path to the CSV file
- `target_table`: (Optional) Fully qualified Databricks table name to sync schema

Returns:
- CSV data
- CSV column names
- Current table columns
- Missing columns (if any)
- Total number of rows

## Notes
- Ensure you have AWS credentials with S3 read access
- Provide Databricks connection details in .env
- Temporary files are created and deleted during CSV reading
- Automatically updates Databricks table schema if new columns are found

The project allows you to:

## To use this project:

Install dependencies: pip install -r requirements.txt
C- opy .env.example to .env 
- Run the server: uvicorn src.main:app --reload 