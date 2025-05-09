# from datasets import load_dataset
# import boto3, io, os, time
# import pyarrow as pa, pyarrow.parquet as pq
# from dotenv import load_dotenv
# from itertools import islice

# load_dotenv()

# aws_access_key  = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secret_key  = os.getenv('AWS_SECRET_ACCESS_KEY')
# aws_region      = os.getenv('AWS_REGION', 'us-east-1')
# bucket_name     = os.getenv('S3_BUCKET_NAME', 'aws-amazon-review')


# # Constants
# CHUNK_SIZE  = 100_000      # records per chunk
# NUM_FILES   = 3           # how many parquet files you want ~700 MB each

# # Initialize S3 client once
# s3 = boto3.client(
#     's3',
#     aws_access_key_id     = aws_access_key,
#     aws_secret_access_key = aws_secret_key,
#     region_name           = aws_region
# )

# ds_stream = load_dataset("McAuley-Lab/Amazon-Reviews-2023", "raw_meta_Beauty_and_Personal_Care", split="full", trust_remote_code=True)

# # Get the first chunk to determine the schema
# first_chunk = []
# for i, example in enumerate(islice(ds_stream, 150_000)):
#     first_chunk.append(example)

# # Create the schema from the first chunk
# schema_table = pa.Table.from_pylist(first_chunk)
# base_schema = schema_table.schema

# def process_range(s3_key):
#     """Stream records [start_idx, end_idx) and write them to S3 under s3_key."""
#     # (Re-)load the streaming dataset

#     buffer = io.BytesIO()
#     writer = None
#     chunk = []
#     chunk_count = 0

#     for i, example in enumerate(ds_stream, start=0):
#         chunk.append(example)
#         if len(chunk) >= CHUNK_SIZE:
#             table = pa.Table.from_pylist(chunk, schema=base_schema)
#             if writer is None:
#                 writer = pq.ParquetWriter(
#                     buffer,
#                     base_schema,
#                     compression='zstd',
#                     compression_level=5,
#                     data_page_size=1_048_576
#                 )
#             writer.write_table(table)
#             chunk = []
#             chunk_count += 1
#             print(f"  → Wrote chunk #{chunk_count}")
    
#     # flush any remainder
#     if chunk:
#         table = pa.Table.from_pylist(chunk, schema=base_schema)
#         if writer is None:
#             writer = pq.ParquetWriter(buffer, base_schema, compression='zstd', compression_level=5)
#         writer.write_table(table)

#     if writer:
#         writer.close()
#     buffer.seek(0)

#     # upload
#     print(f"Uploading {s3_key} to S3…")
#     s3.upload_fileobj(buffer, bucket_name, s3_key)
#     print(f"Uploaded {s3_key}")

# # Measure total time
# start_time = time.time()

# # Process the entire dataset in one go
# s3_key = 'meta_data.parquet'

# process_range(s3_key)

# # Measure elapsed time
# elapsed = time.time() - start_time

# print(f"Total time taken: {elapsed:.2f} seconds")

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
NUM_FILES   = 3           # how many parquet files you want ~700 MB each

# Initialize S3 client once
s3 = boto3.client(
    's3',
    aws_access_key_id     = aws_access_key,
    aws_secret_access_key = aws_secret_key,
    region_name           = aws_region
)

# First, get a schema from a representative sample
print("Extracting schema from sample data...")
ds_schema = load_dataset("McAuley-Lab/Amazon-Reviews-2023", "raw_meta_Beauty_and_Personal_Care", split="full", trust_remote_code=True)
first_chunk = []
for i, example in enumerate(islice(ds_schema, 150_000)):
    first_chunk.append(example)

# Create the schema from the first chunk
schema_table = pa.Table.from_pylist(first_chunk)
base_schema = schema_table.schema
print("Schema extracted successfully")

def process_range(s3_key):
    """Stream records and write them to S3 under s3_key."""
    # Load a fresh copy of the dataset for processing
    ds_stream = load_dataset("McAuley-Lab/Amazon-Reviews-2023", "raw_meta_Beauty_and_Personal_Care", split="full", trust_remote_code=True)

    buffer = io.BytesIO()
    # Initialize writer immediately with the base schema
    writer = pq.ParquetWriter(
        buffer,
        base_schema,
        compression='zstd',
        compression_level=5,
        data_page_size=1_048_576
    )
    
    chunk = []
    chunk_count = 0

    for i, example in enumerate(ds_stream, start=0):
        chunk.append(example)
        if len(chunk) >= CHUNK_SIZE:
            # Always use the base schema
            table = pa.Table.from_pylist(chunk, schema=base_schema)
            writer.write_table(table)
            chunk = []
            chunk_count += 1
            print(f"  → Wrote chunk #{chunk_count}")
    
    # Flush any remainder
    if chunk:
        # Important: Also use the base schema here
        table = pa.Table.from_pylist(chunk, schema=base_schema)
        writer.write_table(table)
        print(f"  → Wrote final chunk")

    writer.close()
    buffer.seek(0)

    # Upload
    print(f"Uploading {s3_key} to S3...")
    s3.upload_fileobj(buffer, bucket_name, s3_key)
    print(f"Uploaded {s3_key}")

# Measure total time
start_time = time.time()

# Process the entire dataset in one go
s3_key = 'meta_data.parquet'

process_range(s3_key)

# Measure elapsed time
elapsed = time.time() - start_time

print(f"Total time taken: {elapsed:.2f} seconds")