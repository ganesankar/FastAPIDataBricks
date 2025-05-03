import os
import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(title="S3 CSV Reader")

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-2')
S3_BUCKET = os.getenv('S3_BUCKET')

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

@app.get("/read_csv")
async def read_csv_from_s3(file_path: str):
    """
    Read a CSV file from S3 and return its contents.
    
    :param file_path: Path to the CSV file in the S3 bucket
    :return: DataFrame contents as a list of dictionaries
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
        
        return {
            "data": df.to_dict(orient='records'),
            "columns": list(df.columns),
            "total_rows": len(df)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
