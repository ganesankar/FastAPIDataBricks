import os
import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from databricks.sql import connect

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(title="S3 CSV Databricks Sync")

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-2')
S3_BUCKET = os.getenv('S3_BUCKET')

# Databricks Configuration
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')

def get_s3_client():
    """Create and return an S3 client."""
    try:
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to S3: {str(e)}")

def get_databricks_connection():
    """Create and return a Databricks SQL connection."""
    try:
        conn = connect(
            server_hostname=DATABRICKS_HOST.replace('https://', ''),
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to Databricks: {str(e)}")

def get_table_schema(table_name):
    """Retrieve the schema of a Databricks table."""
    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            schema = cursor.fetchall()
            return [col[0] for col in schema]

def update_table_schema(table_name, new_columns):
    """Update the schema of a Databricks table by adding missing columns."""
    with get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            for col in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} STRING")
                except Exception as e:
                    print(f"Error adding column {col}: {e}")

@app.get("/read_csv")
async def read_csv_from_s3(file_path: str, target_table: str = "workspace.default.customers"):
    """
    Read a CSV file from S3, compare its schema with a Databricks table,
    and update the table schema if needed.
    
    :param file_path: Path to the CSV file in the S3 bucket
    :param target_table: Fully qualified Databricks table name
    :return: CSV data and schema update details
    """
    try:
        # Create S3 client
        s3_client = get_s3_client()
        
        # Download the file to a temporary location
        local_file_path = os.path.basename(file_path)
        s3_client.download_file(S3_BUCKET, file_path, local_file_path)
        
        # Read CSV using pandas
        df = pd.read_csv(local_file_path)
        
        # Remove the temporary file
        os.remove(local_file_path)
        
        # Get current table schema from Databricks
        current_schema = get_table_schema(target_table)
        
        # Find missing columns
        missing_columns = [col for col in df.columns if col not in current_schema]
        
        # Update table schema if needed
        if missing_columns:
            update_table_schema(target_table, missing_columns)
        
        return {
            "data": df.to_dict(orient='records'),
            "csv_columns": list(df.columns),
            "table_columns": current_schema,
            "missing_columns": missing_columns,
            "total_rows": len(df)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
