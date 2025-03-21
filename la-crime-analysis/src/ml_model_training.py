from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
import os



# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize a BigQuery client
client = bigquery.Client()

query = """
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`
"""

df = client.query(query).to_dataframe()

# Step 1 -- Data Preprocessing (Feature Engineering)

# Drop unnecessary columns
df = df.drop(columns=['dr_no', 'date_occ', 'date_rptd'])
df = df.drop_duplicates()

# Frequency encoding for high-cardinality columns
high_cardinality_columns = ['mocodes', 'location', 'cross_street']

# print("Columns in DataFrame before encoding:", df.columns)

for col in high_cardinality_columns:
    if col in df.columns:  # Check if column exists
        frequency_encoding = df[col].value_counts(normalize=True)
        df[f'{col}_encoded'] = df[col].map(frequency_encoding)
        df[f'{col}_encoded'] = df[f'{col}_encoded'].fillna(0)
    else:
        print(f"Warning: Column '{col}' not found in DataFrame.")



# Label encoding for low-cardinality columns
low_cardinality_columns = ['premis_desc', 'weapon_desc']
label_encoder = LabelEncoder()
for col in low_cardinality_columns:
    df[f'{col}_encoded'] = label_encoder.fit_transform(df[col])

# One-hot encoding for categorical columns
df = pd.get_dummies(df, columns=['area_name', 'vict_descent', 'status_desc'])

# Label encoding for categorical columns
label_encoder = LabelEncoder()
df['crm_cd_desc'] = label_encoder.fit_transform(df['crm_cd_desc'])
df['vict_sex'] = label_encoder.fit_transform(df['vict_sex'])
df['status'] = label_encoder.fit_transform(df['status'])


non_numeric_columns = df.select_dtypes(include=['object']).columns
df = df.drop(columns=non_numeric_columns)



# Define features (X) and target (y)
X = df.drop(columns=['crm_cd_desc'])
y = df['crm_cd_desc']



# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train['crm_cd_1'] = X_train['crm_cd_2'].fillna(0)
X_train['crm_cd_2'] = X_train['crm_cd_2'].fillna(0)
X_train['crm_cd_3'] = X_train['crm_cd_3'].fillna(0)
X_train['crm_cd_4'] = X_train['crm_cd_4'].fillna(0)
X_train['age_missing'] = X_train['vict_age'].isnull().astype(int)
X_train['vict_age'] = X_train['vict_age'].fillna(X_train['vict_age'].median())  # Or use the mean/advanced imputation

print("Data preprocessing complete. Training and test sets are ready.")


# Step 2 -- Train a Machine Learning Model(Logistic Regression model)
model = LogisticRegression(max_iter=1000, verbose=1)

# model.fit(X_train, y_train)

# y_pred = model.predict(X_test)

# accuracy = accuracy_score(y_test, y_pred)
# print(f"Model Accuracy: {accuracy:.2f}")

import time
start_time = time.time()

# Train the model
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

end_time = time.time()
print(f"Total time taken: {end_time - start_time:.2f} seconds")
