import os
from google.cloud import bigquery

# Explicitly set the environment variable in Python script
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize BigQuery client
client = bigquery.Client()

# Test the connection by listing available datasets
try:
    datasets = list(client.list_datasets())  # List datasets in the current project
    print("Connected to BigQuery!")
    for dataset in datasets:
        print(f"Dataset ID: {dataset.dataset_id}")
except Exception as e:
    print(f"Failed to connect: {e}")
