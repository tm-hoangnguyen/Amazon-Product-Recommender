# Amazon Product Recommender

This project implements an Amazon product recommender that allows users to input a query, which is then vectorized and compared against all product reviews to identify the most relevant products. The system returns products ranked by number of reviews and rating scores.

**Dataset:** [McAuley Lab – Amazon Reviews 2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023)

We focus on the **Beauty and Personal Care** category, containing ~**1M items** and **21M reviews**.

**Tech Stack:** Python, S3, Snowflake, Spark (see diagram below)

<img width="1081" height="741" alt="architecture" src="https://github.com/user-attachments/assets/8b25daa7-5301-44f9-9318-3546b1615dfb" />

## Workflow

1. **Data Ingestion** – All raw data is converted to Parquet format and uploaded to Amazon S3 for staging.  
2. **Embedding Generation** – Reviews are embedded using **`all-MiniLM-L6-v2`**, producing 384-dimensional dense vectors via Spark, stored back in S3.  
3. **Metadata Storage** – Product metadata is copied to Snowflake for faster querying.  
4. **Vector Indexing** – Facebook AI Similarity Search (FAISS) indexes all review vectors for efficient similarity search.  
5. **Query Processing** – User queries are vectorized, compared against the FAISS index, and relevant reviews are retrieved.  
6. **Ranking** – Products are ranked using a Bayesian Average of review counts and rating scores to balance popularity and quality.



