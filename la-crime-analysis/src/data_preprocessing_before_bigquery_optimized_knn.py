# Import necessary libraries
import polars as pl  # Polars is a fast DataFrame library for data manipulation, similar to pandas.
from sklearn.impute import KNNImputer  # KNNImputer from scikit-learn is used to fill missing values in numerical data
# using K-Nearest Neighbors (KNN) algorithm.
import logging  # Logging is used to track the progress, warnings, and errors during the script's execution.
import time  # Time is used to measure the elapsed time of operations to track performance.
from datetime import timedelta  # To convert the elapsed time (in seconds) to a human-readable format.
import os  # OS library is used for interacting with the file system (e.g., creating directories, handling paths).
import sys  # Sys library provides access to system-specific parameters, like printing log messages to the console.

# Set up logging to track progress and errors in the script
log_dir = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)  # Create the log directory if it doesn't exist

# Set up logging configuration
log_file_path = os.path.join(log_dir, "data_cleaning.log")  # Specify the path where the logs will be saved
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO, which means logs with INFO or higher severity will be recorded.
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format with timestamp, log level, and message.
    handlers=[
        logging.FileHandler(log_file_path, encoding="utf-8"),  # Log messages to a file with UTF-8 encoding.
        logging.StreamHandler(sys.stdout)  # Also print log messages to the console.
    ]
)

logging.info("Data cleaning started.")  # Log that the data cleaning process has started.

try:
    # Load the dataset using Polars
    file_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\data\raw_data\la_crime_data.csv"
    df = pl.read_csv(file_path)  # Load the CSV file into a Polars DataFrame
    logging.info("Dataset loaded successfully.")  # Log a success message after loading the dataset
except Exception as e:
    logging.error(f"Error loading the dataset: {e}")  # Log an error message if there's an issue
    raise  # Stop the script if the dataset cannot be loaded

# Track the start time of the entire data cleaning process
start_time = time.time()

# Total number of records in the dataset (for progress tracking)
total_records = len(df)
logging.info(f"Total records in the dataset: {total_records}")  # Log the number of records in the dataset

# Function to log progress during data cleaning
def log_progress(step_name, start_time, processed_records=None, total_records=None):
    """
    Logs the progress of data cleaning steps, showing percentage completion and elapsed time.
    """
    elapsed_time = time.time() - start_time  # Calculate the time elapsed since the start
    if processed_records is not None and total_records is not None:
        percentage_complete = (processed_records / total_records) * 100  # Calculate completion percentage
        logging.info(f"Completed step: {step_name}")  # Log the step name
        logging.info(f"Progress: {percentage_complete:.2f}% completed ({processed_records}/{total_records} records).")  # Log progress
    else:
        logging.info(f"Completed step: {step_name}")  # Log step name if no progress tracking is needed
    logging.info(f"Elapsed time: {str(timedelta(seconds=int(elapsed_time)))}.")  # Log the elapsed time in human-readable format

# Step 1: Handle Missing Values
logging.info("Handling missing values...")  # Log that we're starting the process of handling missing values

try:
    # Check for missing values in each column and print the result
    print("Missing Values:\n", df.null_count())  # This will print the number of missing values per column

    # Fill missing categorical values with "Unknown" and cast them to the 'category' type for memory efficiency
    categorical_cols = ["Vict Sex", "Vict Descent", "Premis Desc"]
    df = df.with_columns([  #  This method is used to add or modify columns in the DataFrame df.
        pl.col(col).fill_null("Unknown").cast(pl.Categorical) for col in categorical_cols  # Replace nulls with "Unknown" for these columns
    ])  # The Categorical data type is used for columns that have a limited, fixed set of unique values
        # (e.g., categories like "Red", "Blue", "Green").
    logging.info("Missing categorical values handled successfully.")  # Log success message
except Exception as e:
    logging.error(f"Error handling missing categorical values: {e}")  # Log error if something goes wrong
    raise  # Stop the script if there's an error

# Step 2: Use KNN Imputer for Imputation on Numerical Columns
logging.info("Imputing missing numerical values using KNN imputer...")  # Log the start of numerical imputation

try:
    # List of numerical columns to impute (e.g., missing values in age, crime codes, etc.)
    numerical_cols = ["Vict Age", "Premis Cd", "Weapon Used Cd"]

    # Initialize the KNNImputer with 7 neighbors (K=7 means it will consider the 7 nearest neighbors for imputation)
    imputer = KNNImputer(n_neighbors=7)

    # Convert numerical columns to a NumPy array for faster imputation
    numerical_data = df[numerical_cols].to_numpy()

    # Track progress and impute in chunks
    chunk_size = 100000  # Process every 1,00,000 records to avoid memory overload
    processed_records = 0

    # Apply KNN imputation in chunks
    for i in range(0, len(numerical_data), chunk_size):
        chunk = numerical_data[i:i + chunk_size]  # Get a chunk of the data (0:0 + 100000) (Zero to Zero + 100000)
        numerical_data[i:i + chunk_size] = imputer.fit_transform(chunk)  # Apply KNN imputation
        processed_records += len(chunk)  # Track the number of processed records
        log_progress("KNN Imputation", start_time, processed_records, total_records)  # Log progress after each chunk

    # Convert the imputed data back to Polars DataFrame
    df = df.with_columns([  # with_columns is a Polars method that takes a list of new or updated columns and applies them to the DataFrame.
        pl.Series(numerical_cols[i], numerical_data[:, i]) for i in range(len(numerical_cols))
    ])
    # Each column is defined as a pl.Series object.
    # numerical_cols[i]: The name of the column (from the numerical_cols list).
    # numerical_data[:, i]: Extracts the i-th column from the numerical_data NumPy array.
    # numerical_data[:, i] uses NumPy slicing to get all rows (:) for the i-th column.
    # pl.Series(...): Converts the NumPy array column into a Polars Series.

    # Ensure there are no remaining missing values in the numerical columns
    null_counts = df[numerical_cols].null_count()  # Check for any remaining null values
    total_nulls = null_counts.sum().row(0)[0]  # Sum up the null counts
    # .row(0)[0]: Extracts the first value from the first row of the summed DataFrame.
    # This represents the total number of nulls across all columns.
    # The sum() method aggregates the null counts, and .row(0)[0] extracts the total count for further validation.
    assert total_nulls == 0, f"Missing values still exist in numerical columns: {null_counts}"  # Assert no missing values
    # assert: A Python keyword that checks if a condition is True. If the condition is False, it raises an AssertionError.
    logging.info("KNN imputation completed successfully.")  # Log success message
except Exception as e:
    logging.error(f"Error during KNN imputation: {e}")  # Log error if there's an issue with imputation
    raise  # Stop the script if there's an error

# Step 3: Format Dates and Times
logging.info("Formatting dates and times...")  # Log that we're starting date formatting

try:
    # Convert 'Date Rptd' and 'DATE OCC' columns to datetime format (from string to datetime objects)
    df = df.with_columns([
        pl.col("Date Rptd").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p"),
        pl.col("DATE OCC").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p")
    ])

    # Extract additional features from the datetime columns, such as the hour of the day, weekday, and month
    df = df.with_columns([
        (pl.col("TIME OCC").cast(pl.Utf8).str.slice(0, 2).cast(pl.Int32).alias("hour_of_day")),
        # .cast(pl.Utf8) ensures that the column is treated as a string, which is necessary for string operations like slicing.
        # .str.slice(0, 2) slices the string from index 0 to 2 (exclusive), effectively extracting the first two characters.
        # .cast(pl.Int32) converts the string (e.g., "12" or "04") to a 32-bit integer.
        pl.col("DATE OCC").dt.weekday().alias("day_of_week"),
        pl.col("DATE OCC").dt.month().alias("month")
    ])
    logging.info("Date and time formatting completed successfully.")  # Log success message
except Exception as e:
    logging.error(f"Error formatting dates and times: {e}")  # Log error if date formatting fails
    raise  # Stop the script if there's an error

# Step 4: Encode Categorical Variables using Target Encoding
logging.info("Encoding categorical variables using Target Encoding...")  # Log the start of categorical encoding

try:
    # Calculate the mean of 'Crm Cd' for each unique value in 'AREA NAME' (this is called target encoding)
    area_name_encoded = df.group_by("AREA NAME").agg(pl.col("Crm Cd").mean().alias("area_name_encoded"))
    # group_by("AREA NAME") groups the rows of the DataFrame based on the unique values in the "AREA NAME" column.
    # .agg() performs an aggregation operation on the grouped data.
    # pl.col("Crm Cd").mean() calculates the mean of the "Crm Cd" column for each group.
    # .alias("area_name_encoded") renames the resulting column to "area_name_encoded".

    # Join the encoded values back to the original DataFrame
    df = df.join(area_name_encoded, on="AREA NAME", how="left")
    # The join method combines rows from two DataFrames based on a key column ("AREA NAME" in this case).
    # The join operation matches rows in df and area_name_encoded where the values in the "AREA NAME" column are equal.
    # A left join keeps all rows from the left DataFrame (df) and adds matching rows from the right DataFrame (area_name_encoded).
    # If there is no match in area_name_encoded, the "area_name_encoded" column will contain null for that row.
    logging.info("Categorical variable encoding completed successfully.")  # Log success message
except Exception as e:
    logging.error(f"Error encoding categorical variables: {e}")  # Log error if encoding fails
    raise  # Stop the script if there's an error

# Step 5: Rename Columns to Standard Format
logging.info("Renaming columns to a standard format...")  # Log the start of column renaming

try:
    # Function to clean and standardize column names (e.g., converting spaces to underscores, making everything lowercase)
    def clean_column_name(col):
        if not isinstance(col, str):  # Ensure the column name is a string
            col = str(col)  # Convert to string if not already
        # Replace spaces with underscores and convert to lowercase
        col = col.lower().replace(" ", "_")
        # Remove or replace any special characters (optional)
        col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
        # c.isalnum(): Checks if the character c is alphanumeric (i.e., a letter or a number).
        # c == "_": Checks if the character is an underscore.
        # If the character is alphanumeric or an underscore, it is kept as is.
        # Otherwise, it is replaced with an underscore (_).
        # The join method concatenates all the characters (or underscores) generated by the generator expression into a single string.
        return col

    # Apply the cleaning function to all column names
    rename_dict = {col: clean_column_name(col) for col in df.columns}
    # rename_dict = {"old_col1": "new_col1", "old_col2": "new_col2", "old_col3": "new_col1"} --  dictionary

    # Check for duplicate column names after renaming
    new_columns = list(rename_dict.values())
    # new_columns = list(rename_dict.values())  # ['new_col1', 'new_col2', 'new_col1'] -- list

    if len(new_columns) != len(set(new_columns)):  # If there are duplicates
        # set(new_columns) creates a set of unique column names (since sets automatically remove duplicates).
        duplicate_columns = [col for col in new_columns if new_columns.count(col) > 1]
        # new_columns = ['new_col1', 'new_col2', 'new_col1']
        # duplicate_columns = [col for col in new_columns if new_columns.count(col) > 1]
        # # Result: ['new_col1', 'new_col1']
        raise ValueError(f"Duplicate column names detected after renaming: {duplicate_columns}")

    # Rename columns in the DataFrame
    df = df.rename(rename_dict)
    logging.info("Column renaming completed successfully.")  # Log success message
except Exception as e:
    logging.error(f"Error renaming columns: {e}")  # Log error if renaming fails
    raise  # Stop the script if there's an error

# Step 6: Save the Cleaned Data
logging.info("Saving the cleaned data...")  # Log that the cleaned data is being saved

try:
    # Specify the output file path where the cleaned data will be saved as a Parquet file
    output_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\data\processed_data\pre_cleaned_crime_data.parquet"
    df.write_parquet(output_path)  # Save the DataFrame to the specified Parquet file
    logging.info(f"Cleaned data saved successfully to {output_path}.")  # Log success message
except Exception as e:
    logging.error(f"Error saving cleaned data: {e}")  # Log error if saving fails
    raise  # Stop the script if there's an error

# Final Progress Update
elapsed_time = time.time() - start_time  # Calculate total elapsed time
logging.info(f"Data cleaning completed: 100% completed.")  # Log that the cleaning is complete
logging.info(f"Total elapsed time: {str(timedelta(seconds=int(elapsed_time)))}.")  # Log total elapsed time
