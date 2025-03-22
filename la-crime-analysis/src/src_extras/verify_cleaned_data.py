import pandas as pd

# Load the cleaned data
df = pd.read_parquet('C:/Users/Anitta/data-portfolio-projects/la-crime-analysis/data/processed_data/pre_cleaned_crime_data.parquet')

# Step 1: Check for missing values
print("Missing Values:")
print(df.isnull().sum())

# Step 2: Check data types
print("\nData Types:")
print(df.dtypes)

# Step 3: Verify date and time formatting
print("\nDate and Time Columns:")
print(df[['date_occ', 'time_occ']].head())

# Step 4: Inspect categorical encoding
print("\nUnique Values in Categorical Columns:")
print("area_name:", df['area_name'].unique())
print("vict_sex:", df['vict_sex'].unique())

# Step 5: Check numerical columns
print("\nSummary Statistics for Numerical Columns:")
print(df[['vict_age', 'lat', 'lon']].describe())

# Step 6: Validate feature engineering
print("\nUnique Values in New Features:")
print("hour_of_day:", df['hour_of_day'].unique())
print("day_of_week:", df['day_of_week'].unique())

# Step 7: Verify column renaming
print("\nColumn Names:")
print(df.columns)

# Step 8: Check for duplicates
print("\nNumber of Duplicates:")
print(df.duplicated().sum())

# Step 9: Sample data inspection
print("\nRandom Sample of Data:")
print(df.sample(5))