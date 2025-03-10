from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import csv
import os

# Set up options for headless mode
options = Options()
# options.headless = True  # Test without headless mode first

# Specify the full path to ChromeDriver
chrome_driver_path = r'C:\Users\Anitta\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe'

# Set up the WebDriver using the Service class
service = Service(chrome_driver_path)  # Create a Service object
driver = webdriver.Chrome(service=service, options=options)  # Pass the Service object

# URL of the IMDb Box Office page
url = "https://www.imdb.com/chart/boxoffice"

# Open the URL
print("Opening IMDb...")
driver.get(url)

# Wait for the list containing the box office data to appear
print("Waiting for list to load...")
try:
    movie_list = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "ipc-metadata-list"))
    )
    print("List found.")
except Exception as e:
    print(f'Error: {e}')
    driver.quit()
    exit()

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(driver.page_source, 'html.parser')

# Find the list containing the box office data
movie_list = soup.find('ul', class_='ipc-metadata-list')

# Check if the list was found
if not movie_list:
    print("Could not find the box office list.")
    driver.quit()
    exit()

# Find all movie items in the list
movies = movie_list.find_all('li', class_='ipc-metadata-list-summary-item')

# Define the path to save the scraped data
raw_data_path = os.path.join('..', 'data', 'raw', 'boxoffice_data.csv')

# Ensure the 'data/raw' directory exists
os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)

# Open a CSV file for writing
with open(raw_data_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Write the header row
    writer.writerow(['Title', 'Weekend Gross', 'Total Gross', 'Weeks'])

    # Loop through each movie and extract the data
    for movie in movies:
        try:
            # Extract the movie title
            title = movie.find('h3', class_='ipc-title__text').text.strip()

            # Extract the weekend gross, total gross, and weeks
            metadata = movie.find_all('li', class_='sc-8f57e62c-1')
            weekend_gross = metadata[0].find('span', class_='sc-8f57e62c-2').text.strip()
            total_gross = metadata[1].find('span', class_='sc-8f57e62c-2').text.strip()
            weeks = metadata[2].find('span', class_='sc-8f57e62c-2').text.strip()

            # Write the data to CSV file
            writer.writerow([title, weekend_gross, total_gross, weeks])
        except AttributeError as e:
            print(f"Skipping a movie due to missing data: {e}")

    print(f'Data saved to {raw_data_path}')

# Close the browser
driver.quit()