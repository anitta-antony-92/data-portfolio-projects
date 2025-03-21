import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.impute import SimpleImputer
from sklearn.feature_selection import VarianceThreshold
from sklearn.decomposition import PCA
import os
import psutil
import time

# Function to monitor system health
def monitor_system_health():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_info = psutil.virtual_memory()
    disk_usage = psutil.disk_usage('/')
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
LIMIT 50000
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
y = y.fillna(y.mode()[0])

# Handle missing values in X
imputer = SimpleImputer(strategy='median')
X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)

# Remove constant features
constant_filter = VarianceThreshold(threshold=0)
X = constant_filter.fit_transform(X)

# Feature Scaling
scaler = StandardScaler()
X = scaler.fit_transform(X)

# Dimensionality Reduction using PCA
pca = PCA(n_components=20)  # Reduce to 20 components
X = pca.fit_transform(X)

# Split the data into training and test sets (80% train, 20% test)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
print("Number of features after preprocessing and selection:", X_train.shape[1])

# Check memory usage after preprocessing
print("Post-processing memory usage:", df.memory_usage(deep=True).sum() / (1024 ** 2), "MB")

# Monitor system health before model training
print("System Health Before Model Training:")
monitor_system_health()

# Print class distribution
print("Class Distribution in y_train:")
print(y_train.value_counts())

print("Class Distribution in y_test:")
print(y_test.value_counts())

# Fit LabelEncoder on the entire target column (y) to ensure it knows all possible classes
label_encoder.fit(y)

# Encode y_train and y_test using the fitted LabelEncoder
y_train_encoded = label_encoder.transform(y_train)
y_test_encoded = label_encoder.transform(y_test)

# Example: Random Forest Model
start_time = time.time()
rf_model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
rf_model.fit(X_train, y_train_encoded)
print(f"Time taken to train Random Forest: {time.time() - start_time:.2f} seconds")

# Predict on the test data
y_pred_encoded = rf_model.predict(X_test)

# Decode the predicted labels back to original class names
y_pred = label_encoder.inverse_transform(y_pred_encoded)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Generate a classification report
print("Classification Report:")
print(classification_report(y_test, y_pred, zero_division=0))

# Monitor system health after model training
print("System Health After Model Training:")
monitor_system_health()

# Calculate and print total runtime
total_runtime = time.time() - total_start_time
print(f"Total Runtime: {total_runtime:.2f} seconds")