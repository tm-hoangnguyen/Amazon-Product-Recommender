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

def read_s3_file(key):
    # key = "data/beauty_personal_care_7000000_14000000.parquet"
    # key = "meta_data.parquet"
    # key = "data_vectors/file_0_7M_0_49999"
    key = key

    # Option 1: Using pyarrow with S3FileSystem (memory efficient)
    s3fs = S3FileSystem(
        access_key=aws_access_key,
        secret_key=aws_secret_key,
        region=aws_region
    )

    # read the whole folder
    parquet_file = pq.ParquetDataset(f"{bucket_name}/{key}", filesystem=s3fs)

    sample_df = parquet_file.read().to_pandas()

    return sample_df

# print(sample_df.head(10)) 

# shape of 1 array: (384,)
