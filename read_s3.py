import boto3
import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
from dotenv import load_dotenv

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')
bucket_name = os.getenv('S3_BUCKET_NAME', 'aws-amazon-review')
# key = "data/beauty_personal_care_7000000_14000000.parquet"
key = "meta_data.parquet"

# Option 1: Using pyarrow with S3FileSystem (memory efficient)
s3fs = S3FileSystem(
    access_key=aws_access_key,
    secret_key=aws_secret_key,
    region=aws_region
)

# Open the file without reading all data
parquet_file = pq.ParquetFile(s3fs.open_input_file(f"{bucket_name}/{key}"))

# Read just the first row group
first_batch = next(parquet_file.iter_batches(batch_size=10))
sample_df = first_batch.to_pandas()
pd.set_option('display.max_columns', None)  # Show all columns
print("First 10 rows:")
print(sample_df)