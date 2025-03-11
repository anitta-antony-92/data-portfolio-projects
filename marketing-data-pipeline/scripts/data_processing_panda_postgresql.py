import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_config = {
    'dbname': 'marketing_data',
    'user': 'postgres',
    'password': 'All!swell492',
    'host': 'localhost',
    'port': 5432
}

# Create a connection string
conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"

# Create a SQLAlchemy engine
engine = create_engine(conn_string)

# Load the boxoffice table into a pandas DataFrame
query = 'SELECT * FROM boxoffice;'
df = pd.read_sql(query, engine)

# Step 1: Clean the data
# Clean the title column (remove numbering and periods)
df['title'] = df['title'].str.replace(r'^\d+\.\s*', '', regex=True)

# Handle missing or invalid values
# Fill missing numeric values with 0
df['weekend_gross'].fillna(0, inplace=True)
df['total_gross'].fillna(0, inplace=True)
df['weeks'].fillna(0, inplace=True)

# Convert numeric columns to appropriate types
df['weekend_gross'] = pd.to_numeric(df['weekend_gross'])
df['total_gross'] = pd.to_numeric(df['total_gross'])
df['weeks'] = pd.to_numeric(df['weeks'])

# Handle edge cases (e.g., weeks = 0)
df['weeks'] = df['weeks'].replace(0, 1)  # Replace 0 with 1 to avoid division by zero

# Step 2: Transform the data
# Calculate average gross per week
df['average_gross_per_week'] = df['total_gross'] / df['weeks']

# Filter out movies with total gross less than $10
df_filtered = df[df['total_gross'] >= 10]

# Sort by total gross in descending order
df_sorted = df_filtered.sort_values(by='total_gross', ascending=False)

# Step 3: Save the transformed data back to PostgreSQL
df_sorted.to_sql('boxoffice_transformed', engine, if_exists='replace', index=False)

print('Data processing and transformation completed!')
