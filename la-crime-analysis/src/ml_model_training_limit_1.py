import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer  # For handling missing values
import os
import psutil  # For system monitoring
import time  # For tracking execution time

# Function to monitor system health
def monitor_system_health():
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage as a percentage
    memory_info = psutil.virtual_memory()  # Memory usage in bytes
    disk_usage = psutil.disk_usage('/')  # Disk usage for the root directory
    print(f"CPU Usage: {cpu_usage}%")
    print(f"Memory Usage: {memory_info.used / (1024 ** 3):.2f} GB / {memory_info.total / (1024 ** 3):.2f} GB")
    print(f"Disk Usage: {disk_usage.used / (1024 ** 3):.2f} GB / {disk_usage.total / (1024 ** 3):.2f} GB")
    print("-" * 40)

# Start tracking total runtime
total_start_time = time.time()

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize a BigQuery client
client = bigquery.Client()

# Your BigQuery query
query = """
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`
LIMIT 50000  # Limiting the dataset to 50,000 rows to avoid memory overload
"""

# Monitor system health before running the query
print("System Health Before Query Execution:")
monitor_system_health()

# Run the query and convert the results into a pandas dataframe
start_time = time.time()
df = client.query(query).to_dataframe()
print(f"Time taken to load data: {time.time() - start_time:.2f} seconds")

# Monitor system health after loading data
print("System Health After Loading Data:")
monitor_system_health()

# Check the initial memory usage of the DataFrame
print("Initial memory usage:", df.memory_usage(deep=True).sum() / (1024 ** 2), "MB")

# Drop unnecessary columns
df = df.drop(columns=['dr_no', 'date_occ', 'date_rptd'])
df = df.drop_duplicates()

# Efficient Encoding for high-cardinality columns (Frequency Encoding)
high_cardinality_columns = ['mocodes', 'location', 'cross_street']
for col in high_cardinality_columns:
    if col in df.columns:
        frequency_encoding = df[col].value_counts(normalize=True)
        df[f'{col}_encoded'] = df[col].map(frequency_encoding).fillna(0)
        df = df.drop(columns=[col])

# Efficient Label Encoding for low-cardinality columns
low_cardinality_columns = ['vict_sex', 'premis_desc', 'weapon_desc', 'status']
label_encoder = LabelEncoder()
for col in low_cardinality_columns:
    if col in df.columns:
        df[f'{col}_encoded'] = label_encoder.fit_transform(df[col])
        df = df.drop(columns=[col])

# One-hot encoding for categorical columns (with drop_first=True to avoid collinearity)
df = pd.get_dummies(df, columns=['area_name', 'vict_descent', 'status_desc'], drop_first=True)

# Optimize column types for memory usage
df['vict_age'] = pd.to_numeric(df['vict_age'], errors='coerce').fillna(df['vict_age'].median())
df['crm_cd_desc'] = df['crm_cd_desc'].astype('category')

# Monitor system health after preprocessing
print("System Health After Preprocessing:")
monitor_system_health()

# Define features (X) and target (y)
X = df.drop(columns=['crm_cd_desc'])
y = df['crm_cd_desc']

# Handle missing values (if any) in the target variable as well
y = y.fillna(y.mode()[0])  # Filling missing target values with the most frequent class

# Handle missing values in X
# Option 1: Drop rows with missing values
# X = X.dropna()

# Option 2: Impute missing values (recommended)
imputer = SimpleImputer(strategy='median')  # Use median for numerical columns
X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)

# Split the data into training and test sets (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print("Number of features after preprocessing:", X_train.shape[1])
# Check memory usage after preprocessing
print("Post-processing memory usage:", df.memory_usage(deep=True).sum() / (1024 ** 2), "MB")

# Monitor system health before model training
print("System Health Before Model Training:")
monitor_system_health()

# Example: Logistic Regression Model
start_time = time.time()
model = LogisticRegression(max_iter=1000, solver='liblinear')
model.fit(X_train, y_train)
print(f"Time taken to train Logistic Regression: {time.time() - start_time:.2f} seconds")

# Predict on the test data
y_pred = model.predict(X_test)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Monitor system health after model training
print("System Health After Model Training:")
monitor_system_health()

# # Optionally, use Random Forest for better performance on classification tasks
# start_time = time.time()
# rf_model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
# rf_model.fit(X_train, y_train)
# print(f"Time taken to train Random Forest: {time.time() - start_time:.2f} seconds")
#
# # Predict with Random Forest
# y_pred_rf = rf_model.predict(X_test)
#
# # Evaluate Random Forest model
# accuracy_rf = accuracy_score(y_test, y_pred_rf)
# print(f"Random Forest Accuracy: {accuracy_rf:.2f}")
#
# # Monitor system health after Random Forest training
# print("System Health After Random Forest Training:")
# monitor_system_health()

# Calculate and print total runtime
total_runtime = time.time() - total_start_time
print(f"Total Runtime: {total_runtime:.2f} seconds")