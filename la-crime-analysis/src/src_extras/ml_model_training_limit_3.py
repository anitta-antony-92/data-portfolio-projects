import pandas as pd
from google.cloud import bigquery
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.impute import SimpleImputer
from sklearn.feature_selection import VarianceThreshold
from sklearn.decomposition import PCA
from scipy.stats import randint
import os

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Anitta\Desktop\Data_Projects\la-crime-analysis-92d8c07454af.json"

# Initialize a BigQuery client
client = bigquery.Client()

# Your BigQuery query
query = """
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`
LIMIT 50000
"""

# Run the query and convert the results into a pandas dataframe
df = client.query(query).to_dataframe()

# Limit dataset size for testing
df = df.sample(n=10000, random_state=42)  # Limit to 10,000 rows for faster testing

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

# Define features (X) and target (y)
X = df.drop(columns=['crm_cd_desc'])
y = df['crm_cd_desc']

# Handle missing values (if any) in the target variable as well
y = y.fillna(y.mode()[0])

# Handle missing values in X
# Identify columns with no observed values
columns_with_no_values = X.columns[X.isna().all()].tolist()

# Drop columns with no observed values
X = X.drop(columns=columns_with_no_values)

# Apply the imputer
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

# Fit LabelEncoder on the entire target column (y) to ensure it knows all possible classes
label_encoder.fit(y)

# Encode y_train and y_test using the fitted LabelEncoder
y_train_encoded = label_encoder.transform(y_train)
y_test_encoded = label_encoder.transform(y_test)

# Define the hyperparameter grid for Randomized Search
param_dist = {
    'n_estimators': randint(50, 200),  # Number of trees in the forest
    'max_depth': [None, 10, 20, 30, 50],  # Maximum depth of the tree
    'min_samples_split': randint(2, 20),  # Minimum number of samples required to split a node
    'min_samples_leaf': randint(1, 10),  # Minimum number of samples required at each leaf node
    'max_features': ['sqrt', 'log2', None],  # Number of features to consider at every split
    'bootstrap': [True, False]  # Whether bootstrap samples are used
}

# Initialize the Random Forest model
rf_model = RandomForestClassifier(random_state=42, n_jobs=-1)

# Check class distribution
print("Class Distribution in y:")
print(y.value_counts())

# Remove or combine rare classes
class_counts = y.value_counts()
rare_classes = class_counts[class_counts < 2].index
df = df[~y.isin(rare_classes)]
X = X[~y.isin(rare_classes)]
y = y[~y.isin(rare_classes)]

# Re-check class distribution
print("Class Distribution in y after removing rare classes:")
print(y.value_counts())

# Use StratifiedKFold for cross-validation
from sklearn.model_selection import StratifiedKFold

cv_strategy = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)

# Update RandomizedSearchCV with StratifiedKFold
random_search = RandomizedSearchCV(
    estimator=rf_model,
    param_distributions=param_dist,
    n_iter=10,  # Reduced the number of iterations
    cv=cv_strategy,  # Use StratifiedKFold
    scoring='accuracy',
    random_state=42,
    n_jobs=2  # Limiting to 2 cores to reduce CPU usage
)

# Perform Randomized Search
random_search.fit(X_train, y_train_encoded)

# Print the best parameters found
print("Best Parameters from Randomized Search:")
print(random_search.best_params_)

# Use the best model from Randomized Search
best_rf_model = random_search.best_estimator_

# Predict on the test data
y_pred_encoded = best_rf_model.predict(X_test)

# Decode the predicted labels back to original class names
y_pred = label_encoder.inverse_transform(y_pred_encoded)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Generate a classification report
print("Classification Report:")
print(classification_report(y_test, y_pred, zero_division=0))