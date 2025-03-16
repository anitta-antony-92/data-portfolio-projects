import requests
import pandas as pd
import os

# URL of the dataset
url  = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"

# Path to save the file
download_path = r"C:\Users\Anitta\Downloads\la_crime_data.csv"

# Ensure the directory exists
os.makedirs(os.path.dirname(download_path), exist_ok=True)

# Download the data
print('Downloading dataset ...')
response = requests.get(url)
with open(download_path, 'wb') as file:
    file.write(response.content)

# Load data into Pandas Dataframe
df = pd.read_csv(download_path)
print('Dataset downloaded and loaded into DataFrame')
print(df.head()) # Print first few rows to verify
