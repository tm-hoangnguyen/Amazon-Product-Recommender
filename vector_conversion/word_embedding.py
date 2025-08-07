"""Spark script to convert review data into parquets using all-MiniLM-L6-v2"""

import os
import logging
import torch
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from time import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, FloatType

start_time = time()

# Suppress logging
logging.getLogger("py4j").setLevel(logging.ERROR)

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')
bucket_name = os.getenv('S3_BUCKET_NAME', 'aws-amazon-review')
key = "data/beauty_personal_care_0_7000000.parquet"

# Optimized Spark configuration for 8-core, 32GB RAM system
spark = (
    SparkSession.builder
      .appName("s3-parquet-embeddings")
      .config("spark.jars.packages",
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262")
      .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
      .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
      .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      # Optimal settings for hardware
      .config("spark.executor.memory", "12g")  # Use ~40% of RAM
      .config("spark.driver.memory", "8g")
      .config("spark.executor.cores", "2")  # 2 cores per executor
      .config("spark.executor.instances", "4")  # 4 executors total
      .config("spark.default.parallelism", "16")  # 2x cores
      .config("spark.sql.shuffle.partitions", "16")
      # Performance optimizations
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read and partition 
df = spark.read.parquet(f"s3a://{bucket_name}/{key}")

## test ##
BATCH_SIZE = 50000
BATCH_NUMBER = 10
sample_df = df.offset(BATCH_NUMBER * BATCH_SIZE).limit(BATCH_SIZE).repartition(8)


# Cache the input data
sample_df.cache()

# Load model optimized for CPU
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
# Set threads for optimal CPU usage
torch.set_num_threads(2)  # 2 threads per executor

bc_model = spark.sparkContext.broadcast(model)

def embed_text_fn(text):
    if text is None or text == "":
        return None
    m = bc_model.value
    emb = m.encode(text, normalize_embeddings=True, convert_to_numpy=True)
    return [float(x) for x in emb]

# Register the UDF
embed_text = udf(embed_text_fn, ArrayType(FloatType()))

# Apply the UDF with caching
df_emb = (sample_df
          .withColumn("embedding", embed_text(col("text")))
          .select("parent_asin", "embedding")
          .cache())  # Cache the results

# Force computation to see actual processing time
record_count = df_emb.count()
print(f"Processed {record_count} records")

# Write to S3
## test
start_row = BATCH_NUMBER * BATCH_SIZE
end_row = start_row + BATCH_SIZE - 1

df_emb.coalesce(1).write.mode("overwrite").parquet(
    f"s3a://{bucket_name}/data_vectors/file_0_7M_{start_row}_{end_row}"
)
###
end_time = time()
print(f"Time taken: {end_time - start_time:.2f} seconds")
spark.stop()
