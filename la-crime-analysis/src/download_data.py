import requests
import pandas as pd
import os


def download_crime_data(output_path):
    """
    Downloads LA crime data from the specified URL

    Args:
        output_path (str): Full path where the CSV should be saved
    """

    # URL of the dataset
    url = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"

    # # Path to save the file
    # download_path = r"C:\Users\Anitta\Downloads\la_crime_data.csv"

    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Download the data
    print('Downloading dataset ...')
    response = requests.get(url)
    with open(output_path, 'wb') as file:
        file.write(response.content)

    # Load data into Pandas Dataframe
    df = pd.read_csv(output_path)
    print('Dataset downloaded and loaded into DataFrame')
    print(df.head())  # Print first few rows to verify
    print(f'Total records downloaded: {len(df)}')
    return output_path  # Return path for downstream tasks
