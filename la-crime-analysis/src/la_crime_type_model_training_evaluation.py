import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.impute import SimpleImputer
import os
import time
import joblib
import logging
import json
import shutil
import pickle

# Setup logging configuration
log_file_path = r'C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\log\la_crime_type_model_training_evaluation.log'
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
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

# Preprocess the data (drop columns, handle missing values, etc.)
df = df.drop(columns=['dr_no', 'date_occ', 'date_rptd'])  # Remove irrelevant columns
df = df.drop_duplicates()  # Remove duplicate rows

# Feature encoding
high_cardinality_columns = ['mocodes', 'location', 'cross_street']
for col in high_cardinality_columns:
    if col in df.columns:
        frequency_encoding = df[col].value_counts(normalize=True)
        df[f'{col}_encoded'] = df[col].map(frequency_encoding).fillna(0)
        df = df.drop(columns=[col])

low_cardinality_columns = ['vict_sex', 'premis_desc', 'weapon_desc', 'status']
label_encoder = LabelEncoder()
for col in low_cardinality_columns:
    if col in df.columns:
        df[f'{col}_encoded'] = label_encoder.fit_transform(df[col])
        df = df.drop(columns=[col])

df = pd.get_dummies(df, columns=['area_name', 'vict_descent', 'status_desc'], drop_first=True)

# Handle missing values in target and features
X = df.drop(columns=['crm_cd_desc'])
y = df['crm_cd_desc']

y = y.fillna(y.mode()[0])

imputer = SimpleImputer(strategy='median')
X = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a Logistic Regression model
logging.info("Training Logistic Regression model...")
model = LogisticRegression(max_iter=1000, solver='liblinear')
model.fit(X_train, y_train)

# Save the trained model
logging.info("Saving the trained model...")
joblib.dump(model, 'la_crime_type_model.pkl')

# Model Evaluation: Evaluate on test data
logging.info("Evaluating the model...")

# Predict on test data
y_pred = model.predict(X_test)

# Calculate accuracy score
accuracy = accuracy_score(y_test, y_pred)
logging.info(f"Model Accuracy: {accuracy:.4f}")

# Generate and log classification report
class_report = classification_report(y_test, y_pred, output_dict=True)
logging.info("Classification Report:\n" + json.dumps(class_report, indent=4))

# Generate and log confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)
logging.info(f"Confusion Matrix:\n{conf_matrix}")

# Save the evaluation metrics to a file
evaluation_metrics = {
    "accuracy": accuracy,
    "classification_report": class_report,
    "confusion_matrix": conf_matrix.tolist(),  # Convert matrix to list for JSON serialization
}

evaluation_metrics_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\model_evaluation_metrics.json"
with open(evaluation_metrics_path, "w") as eval_file:
    json.dump(evaluation_metrics, eval_file, indent=4)

# Deployment: Save model and evaluation metrics for deployment
# Create a directory to store the model and evaluation metrics
deployment_dir = r'C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\deployment_model'
if os.path.exists(deployment_dir):
    shutil.rmtree(deployment_dir)  # Remove old directory if exists
os.makedirs(deployment_dir)

# Move the trained model to the deployment directory
shutil.copy('la_crime_type_model.pkl', os.path.join(deployment_dir, 'model.pkl'))

# Move the evaluation metrics to the deployment directory
shutil.copy(evaluation_metrics_path, os.path.join(deployment_dir, 'evaluation_metrics.json'))

logging.info(f"Model and evaluation metrics saved to {deployment_dir} for deployment.")

# Record the end time
end_time = time.time()
logging.info(f"Process End Time: {time.ctime(end_time)}")
logging.info(f"Total Runtime: {end_time - start_time:.2f} seconds")