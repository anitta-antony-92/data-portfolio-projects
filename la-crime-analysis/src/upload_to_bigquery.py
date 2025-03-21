from google.cloud import bigquery
import pandas as pd
import os
import sys

# Check if pyarrow or fastparquet is installed
try:
    import pyarrow  # Test if pyarrow is installed
except ImportError:
    try:
        import fastparquet  # Test if fastparquet is installed
    except ImportError:
        print("Error: Required library 'pyarrow' or 'fastparquet' is not installed.")
        print("Please install it using: pip install pyarrow")
        sys.exit(1)

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize a BigQuery client
client = bigquery.Client()

# Define the dataset and table name
dataset_id = 'la_crime_dataset'
table_id = 'crime_data'

# Create the dataset if it doesn't exist
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)
    print(f'Dataset {dataset_id} already exists.')
except Exception:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    client.create_dataset(dataset)
    print(f'Created dataset {dataset_id}.')

# Delete the existing table if it exists
table_ref = dataset_ref.table(table_id)
try:
    client.delete_table(table_ref)
    print(f'Deleted existing table {table_id}.')
except Exception as e:
    print(f'Table {table_id} does not exist or could not be deleted: {e}')

# Define the table schema
schema = [
    bigquery.SchemaField('dr_no', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('date_rptd', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('date_occ', 'TIMESTAMP', mode='REQUIRED'),
    bigquery.SchemaField('time_occ', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('area', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('area_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('rpt_dist_no', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('part_1_2', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('crm_cd', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('crm_cd_desc', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('mocodes', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('vict_age', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('vict_sex', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('vict_descent', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('premis_cd', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('premis_desc', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('weapon_used_cd', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('weapon_desc', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('status_desc', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('crm_cd_1', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('crm_cd_2', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('crm_cd_3', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('crm_cd_4', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('location', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('cross_street', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('lat', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('lon', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('hour_of_day', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('day_of_week', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('month', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('area_name_encoded', 'FLOAT', mode='NULLABLE'),
]

# Create the table
try:
    table = client.create_table(bigquery.Table(table_ref, schema=schema))
    print(f'Created table {table_id}.')
except Exception as e:
    print(f'Error creating table: {e}')
    sys.exit(1)

# Load the cleaned data from the Parquet file
file_path = r'C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\data\processed_data\pre_cleaned_crime_data.parquet'
try:
    df = pd.read_parquet(file_path)
except Exception as e:
    print(f'Error reading Parquet file: {e}')
    sys.exit(1)

# Convert date columns to datetime format
df['date_rptd'] = pd.to_datetime(df['date_rptd'])
df['date_occ'] = pd.to_datetime(df['date_occ'])

# Convert float columns to integers
integer_columns = [
    'hour_of_day', 'day_of_week', 'month', 'crm_cd_1', 'crm_cd_2', 'crm_cd_3', 'crm_cd_4', 'weapon_used_cd', 'premis_cd'
]

for col in integer_columns:
    if col in df.columns:
        df[col] = df[col].astype(float).round().astype('Int64')

# Handle missing values in the 'status' column
if 'status' in df.columns:
    missing_status = df['status'].isnull().sum()
    if missing_status > 0:
        df = df.fillna({'status': 'Unknown'})

# Load the data into BigQuery
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    schema=schema,
)

try:
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')
except Exception as e:
    print(f'Error loading data: {e}')