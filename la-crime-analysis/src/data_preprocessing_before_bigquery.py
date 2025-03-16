# Import necessary libraries
import polars as pl
from sklearn.impute import KNNImputer
import logging
import time
from datetime import timedelta
import os
import sys

# Set up logging
log_dir = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file_path = os.path.join(log_dir, "data_cleaning.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("Data cleaning started.")

try:
    # Load the dataset using Polars
    file_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\data\raw_data\la_crime_data.csv"
    df = pl.read_csv(file_path)
    logging.info("Dataset loaded successfully.")
except Exception as e:
    logging.error(f"Error loading the dataset: {e}")
    raise

# Track start time of data cleaning process
start_time = time.time()

# Total number of records
total_records = len(df)
logging.info(f"Total records in the dataset: {total_records}")

# Function to log progress
def log_progress(step_name, start_time, processed_records=None, total_records=None):
    elapsed_time = time.time() - start_time
    if processed_records is not None and total_records is not None:
        percentage_complete = (processed_records / total_records) * 100
        logging.info(f"Completed step: {step_name}")
        logging.info(f"Progress: {percentage_complete:.2f}% completed ({processed_records}/{total_records} records).")
    else:
        logging.info(f"Completed step: {step_name}")
    logging.info(f"Elapsed time: {str(timedelta(seconds=int(elapsed_time)))}.")

# Step 1: Handle Missing Values
logging.info("Handling missing values...")

try:
    print("Missing Values:\n", df.null_count())

    categorical_cols = ["Vict Sex", "Vict Descent", "Premis Desc"]
    df = df.with_columns([
        pl.col(col).fill_null("Unknown").cast(pl.Categorical) for col in categorical_cols
    ])
    logging.info("Missing categorical values handled successfully.")
except Exception as e:
    logging.error(f"Error handling missing categorical values: {e}")
    raise

# Step 2: Use KNN Imputer for Imputation on Numerical Columns
logging.info("Imputing missing numerical values using KNN imputer...")

try:
    numerical_cols = ["Vict Age", "Premis Cd", "Weapon Used Cd"]
    imputer = KNNImputer(n_neighbors=7)
    numerical_data = df[numerical_cols].to_numpy()

    chunk_size = 100000
    processed_records = 0

    for i in range(0, len(numerical_data), chunk_size):
        chunk = numerical_data[i:i + chunk_size]
        numerical_data[i:i + chunk_size] = imputer.fit_transform(chunk)
        processed_records += len(chunk)
        log_progress("KNN Imputation", start_time, processed_records, total_records)

    df = df.with_columns([
        pl.Series(numerical_cols[i], numerical_data[:, i]) for i in range(len(numerical_cols))
    ])

    null_counts = df[numerical_cols].null_count()
    total_nulls = null_counts.sum().row(0)[0]
    assert total_nulls == 0, f"Missing values still exist in numerical columns: {null_counts}"
    logging.info("KNN imputation completed successfully.")
except Exception as e:
    logging.error(f"Error during KNN imputation: {e}")
    raise

# Step 3: Format Dates and Times
logging.info("Formatting dates and times...")

try:
    df = df.with_columns([
        pl.col("Date Rptd").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p"),
        pl.col("DATE OCC").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p")
    ])

    df = df.with_columns([
        (pl.col("TIME OCC").cast(pl.Utf8).str.slice(0, 2).cast(pl.Int32).alias("hour_of_day")),
        pl.col("DATE OCC").dt.weekday().alias("day_of_week"),
        pl.col("DATE OCC").dt.month().alias("month")
    ])
    logging.info("Date and time formatting completed successfully.")
except Exception as e:
    logging.error(f"Error formatting dates and times: {e}")
    raise

# Step 4: Encode Categorical Variables using Target Encoding
logging.info("Encoding categorical variables using Target Encoding...")

try:
    area_name_encoded = df.group_by("AREA NAME").agg(pl.col("Crm Cd").mean().alias("area_name_encoded"))
    df = df.join(area_name_encoded, on="AREA NAME", how="left")
    logging.info("Categorical variable encoding completed successfully.")
except Exception as e:
    logging.error(f"Error encoding categorical variables: {e}")
    raise

# Step 5: Rename Columns to Standard Format
logging.info("Renaming columns to a standard format...")

try:
    def clean_column_name(col):
        if not isinstance(col, str):
            col = str(col)
        col = col.lower().replace(" ", "_")
        col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
        return col

    rename_dict = {col: clean_column_name(col) for col in df.columns}
    new_columns = list(rename_dict.values())
    if len(new_columns) != len(set(new_columns)):
        duplicate_columns = [col for col in new_columns if new_columns.count(col) > 1]
        raise ValueError(f"Duplicate column names detected after renaming: {duplicate_columns}")

    df = df.rename(rename_dict)
    logging.info("Column renaming completed successfully.")
except Exception as e:
    logging.error(f"Error renaming columns: {e}")
    raise

# Step 6: Save the Cleaned Data
logging.info("Saving the cleaned data...")

try:
    output_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\data\processed_data\pre_cleaned_crime_data.parquet"
    df.write_parquet(output_path)
    logging.info(f"Cleaned data saved successfully to {output_path}.")
except Exception as e:
    logging.error(f"Error saving cleaned data: {e}")
    raise

# Final Progress Update
elapsed_time = time.time() - start_time
logging.info(f"Data cleaning completed: 100% completed.")
logging.info(f"Total elapsed time: {str(timedelta(seconds=int(elapsed_time)))}.")
