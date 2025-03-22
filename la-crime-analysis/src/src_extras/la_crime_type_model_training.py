import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer  # For handling missing values
import os
import time  # For tracking execution time
import logging  # Importing logging module

# Setup logging configuration
log_file_path = r'C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\log\model_training_crime_analysis.log'  # Log file path
logging.basicConfig(
    filename=log_file_path,  # Log file
    level=logging.DEBUG,  # Log level (DEBUG for detailed logging)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
)

# Record the start time of the entire process
start_time = time.time()
logging.info(f"Process Start Time: {time.ctime(start_time)}")

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize a BigQuery client
client = bigquery.Client()

# Define the BigQuery query to fetch crime data (limited to 50,000 rows for memory efficiency)
query = """
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`
LIMIT 50000
"""

# Run the query and convert the results into a pandas DataFrame
data_load_start_time = time.time()
logging.info("Running BigQuery query to load data...")
df = client.query(query).to_dataframe()
data_load_end_time = time.time()
logging.info(f"Time taken to load data: {data_load_end_time - data_load_start_time:.2f} seconds")

# Display a sample of the data
logging.info(f"Data sample (first 5 rows):\n{df.head()}")
print(f"Data sample (first 5 rows):\n{df.head()}")

# Drop unnecessary columns and remove duplicate rows
logging.info("Dropping unnecessary columns and removing duplicates...")
df = df.drop(columns=['dr_no', 'date_occ', 'date_rptd'])  # Remove irrelevant columns
df = df.drop_duplicates()  # Remove duplicate rows

# Frequency Encoding for high-cardinality columns (e.g., 'mocodes', 'location', 'cross_street')
high_cardinality_columns = ['mocodes', 'location', 'cross_street']
for col in high_cardinality_columns:
    if col in df.columns:
        logging.info(f"Applying frequency encoding on column: {col}")
        frequency_encoding = df[col].value_counts(normalize=True)  # Calculate frequency of each category
        df[f'{col}_encoded'] = df[col].map(frequency_encoding).fillna(0)  # Map frequencies to the column
        df = df.drop(columns=[col])  # Drop the original column to save memory

# Label Encoding for low-cardinality columns (e.g., 'vict_sex', 'premis_desc', 'weapon_desc', 'status')
low_cardinality_columns = ['vict_sex', 'premis_desc', 'weapon_desc', 'status']
label_encoder = LabelEncoder()
for col in low_cardinality_columns:
    if col in df.columns:
        logging.info(f"Applying label encoding on column: {col}")
        df[f'{col}_encoded'] = label_encoder.fit_transform(df[col])  # Encode categories as integers
        df = df.drop(columns=[col])  # Drop the original column to save memory

# One-hot encoding for categorical columns (with drop_first=True to avoid multicollinearity)
logging.info("Applying one-hot encoding for categorical columns...")
df = pd.get_dummies(df, columns=['area_name', 'vict_descent', 'status_desc'], drop_first=True)

# Optimize column types for memory usage
logging.info("Optimizing column types for memory usage...")
df['vict_age'] = pd.to_numeric(df['vict_age'], errors='coerce').fillna(df['vict_age'].median())
df['crm_cd_desc'] = df['crm_cd_desc'].astype('category')

# Display a summary of the data after preprocessing
logging.info(f"Data info after preprocessing:\n{df.info()}")
print(f"Data info after preprocessing:\n{df.info()}")

# Define features (X) and target (y)
logging.info("Defining features (X) and target (y)...")
X = df.drop(columns=['crm_cd_desc'])
y = df['crm_cd_desc']

# Display the feature names and target column
logging.info(f"Feature columns: {X.columns.tolist()}")
print(f"Feature columns: {X.columns.tolist()}")

# Handle missing values in the target variable (fill with the most frequent class)
logging.info("Handling missing values in the target variable...")
y = y.fillna(y.mode()[0])

# Handle missing values in features using median imputation
logging.info("Handling missing values in features with median imputation...")
imputer = SimpleImputer(strategy='median')  # Use median for numerical columns
X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)  # Apply imputation and keep column names

# Split the data into training and test sets (80% train, 20% test)
logging.info("Splitting data into training and test sets...")
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
logging.info(f"Number of features after preprocessing: {X_train.shape[1]}")

# Display the shape of the training and test sets
logging.info(f"Training set shape: {X_train.shape}, Test set shape: {X_test.shape}")
print(f"Training set shape: {X_train.shape}, Test set shape: {X_test.shape}")

# Train a Logistic Regression model
logging.info("Training Logistic Regression model...")
model_train_start_time = time.time()
model = LogisticRegression(max_iter=1000, solver='liblinear')  # Use liblinear solver for faster convergence
model.fit(X_train, y_train)  # Train the model on the training data
model_train_end_time = time.time()
logging.info(f"Time taken to train Logistic Regression: {model_train_end_time - model_train_start_time:.2f} seconds")

# Predict on the test data
logging.info("Predicting on the test data...")
y_pred = model.predict(X_test)

# Evaluate the model using accuracy
accuracy = accuracy_score(y_test, y_pred)
logging.info(f"Model accuracy: {accuracy:.2f}")

# Record the end time of the entire process
end_time = time.time()
logging.info(f"Process End Time: {time.ctime(end_time)}")

# Calculate and print total runtime
total_runtime = end_time - start_time
logging.info(f"Total Runtime: {total_runtime:.2f} seconds")

# Optional: Print final statements to the console
print(f"Process completed successfully. Total runtime: {total_runtime:.2f} seconds")
