import psycopg2
import csv
import os

# Database connection parameters
db_config = {
    'dbname': 'marketing_data',
    'user': 'postgres',
    'password': 'All!swell492',
    'host': 'localhost',
    'port': 5432
}

# Path to the CSV file
csv_file_path = os.path.join('..', 'data', 'raw', 'boxoffice_data.csv')

# Data cleaning functions


def clean_weekend_gross(value):
    """ Remove '$' and 'M' from the weekend gross valur and convert to numeric."""
    return float(value.replace('$', '').replace('M', ''))


def clean_total_gross(value):
    """Remove '$' and 'M' from the total gross value and convert to numeric."""
    return float(value.replace('$', '').replace('M', ''))


def clean_weeks(value):
    """ Convert weeks to an integer."""
    return int(value)

# Connect to the database


try:
    # Establish the connection
    conn = psycopg2.connect(**db_config)

    # Create a cursor object
    cursor = conn.cursor()
    print("Connected to the database.")

    # Check if the CSV file exists
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f'CSV file not found at: {csv_file_path}')

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header row

        # Validate the header
        expected_header = ['Title', 'Weekend Gross', 'Total Gross', 'Weeks']
        if header != expected_header:
            raise ValueError(f'CSV header mismatch. Expected: {expected_header}, Found: {header}')

        for row in reader:
            # Validate the number of columns in each row
            if len(row) != 4:
                raise ValueError(f'Row has incorrect number of columns: {row}')

            title, weekend_gross, total_gross, weeks = row

            # Clean and transfer the data
            try:
                weekend_gross = clean_weekend_gross(weekend_gross)
                total_gross = clean_total_gross(total_gross)
                weeks = clean_weeks(weeks)
            except ValueError as e:
                print(f'Skipping row due to invalid data: {row}')
                continue

            insert_query = """
                INSERT INTO boxoffice (title, weekend_gross, total_gross, weeks)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_query, (title, weekend_gross, total_gross, weeks))
            conn.commit()
        print("Data inserted into the database.")
except FileNotFoundError as e:
    print(f'Error: {e}')
except psycopg2.Error as e:
    print(f'Database Error: {e}')
except ValueError as e:
    print(f'Validation Error: {e}')
except Exception as e:
    print(f'Error inserting data: {e}')
finally:
    # Close the database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("Database connection closed.")
