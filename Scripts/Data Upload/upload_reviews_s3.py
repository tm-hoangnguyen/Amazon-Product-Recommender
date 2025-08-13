'''Retrieve and upload ~21M rows of actual review data for Beauty and Personal Care data,
and convert to parquet before uploading to S3'''
from datasets import load_dataset
import boto3, io, os, time
import pyarrow as pa, pyarrow.parquet as pq
from dotenv import load_dotenv
from itertools import islice

load_dotenv()

aws_access_key  = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key  = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region      = os.getenv('AWS_REGION', 'us-east-1')
bucket_name     = os.getenv('S3_BUCKET_NAME', 'aws-amazon-review')

# Constants
CHUNK_SIZE  = 100_000      # records per chunk
FILE_SIZE   = 7_000_000   # records per parquet file: 7M, total: 21M
NUM_FILES   = 3           # how many parquet files you want ~700 MB each

# Initialize S3 client once
s3 = boto3.client(
    's3',
    aws_access_key_id     = aws_access_key,
    aws_secret_access_key = aws_secret_key,
    region_name           = aws_region
)

def process_range(start_idx, end_idx, s3_key):
    """Stream records [start_idx, end_idx) and write them to S3 under s3_key."""
    # reload the streaming dataset
    ds_stream = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        "raw_review_Beauty_and_Personal_Care",
        streaming=True,
        trust_remote_code=True
    )['full']
    
    # Slice out just this segment
    sliced = islice(ds_stream, start_idx, end_idx)
    
    buffer = io.BytesIO()
    writer = None
    chunk = []
    chunk_count = 0

    for i, example in enumerate(sliced, start=start_idx):
        chunk.append(example)
        if len(chunk) >= CHUNK_SIZE:
            table = pa.Table.from_pylist(chunk)
            if writer is None:
                writer = pq.ParquetWriter(
                    buffer,
                    table.schema,
                    compression='zstd',
                    compression_level=5,
                    data_page_size=1_048_576
                )
            writer.write_table(table)
            chunk = []
            chunk_count += 1
            print(f"  → Wrote chunk #{chunk_count} for range {start_idx}-{end_idx}")
    
    # flush any remainder
    if chunk:
        table = pa.Table.from_pylist(chunk)
        if writer is None:
            writer = pq.ParquetWriter(buffer, table.schema, compression='zstd', compression_level=5)
        writer.write_table(table)

    if writer:
        writer.close()
    buffer.seek(0)

    # upload
    print(f"Uploading {s3_key} to S3…")
    s3.upload_fileobj(buffer, bucket_name, s3_key)
    print(f"Uploaded {s3_key}")

# Measure total time
start_time = time.time()

for file_idx in range(NUM_FILES):
    start = file_idx * FILE_SIZE
    end   = (file_idx + 1) * FILE_SIZE
    key   = f"data/beauty_personal_care_{start}_{end}.parquet"
    process_range(start, end, key)

elapsed = time.time() - start_time
print(f"All done in {elapsed:.2f}s")
