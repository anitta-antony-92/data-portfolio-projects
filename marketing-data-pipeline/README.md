# Marketing Data Pipeline & Dashboard

## Project Overview
The goal of this project is to build a **data pipeline** that collects, processes, and visualizes marketing data (e.g., box office performance) to generate actionable insights. The pipeline includes the following steps:

1. **Data Collection**: Web scraping to collect box office data from IMDb.
2. **Data Storage**: Storing the scraped data in a PostgreSQL database.
3. **Data Processing & Transformation**: Cleaning and transforming the data for analysis.
4. **Data Visualization**: Creating visualizations to analyze the data.
5. **Automation & Deployment**: Automating the pipeline and deploying the dashboard.



---


### 1. **Data Collection (Web Scraping)**
- **Objective**: Collect box office data from IMDb.
- **Tools Used**: Python, Selenium, BeautifulSoup.
- **Script**: [`web_scraping.py`](scripts/web_scraping.py)
- **Output**: A CSV file (`boxoffice_data.csv`) containing the scraped data.

#### Sample Data
| Title                                | Weekend Gross | Total Gross | Weeks |
|--------------------------------------|---------------|-------------|-------|
| 1. Mickey 17                         | $19M          | $19M        | 1     |
| 2. Captain America: Brave New World  | $8.5M         | $177M       | 4     |
| 3. Last Breath                       | $4.2M         | $15M        | 2     |
| 4. The Monkey                        | $3.9M         | $31M        | 3     |
| 5. Paddington in Peru                | $3.9M         | $37M        | 4     |

---

### 2. **Data Storage (PostgreSQL)**
- **Objective**: Store the scraped data in a PostgreSQL database.
- **Tools Used**: PostgreSQL, Python (`psycopg2`).
- **Script**: [`load_csv_to_db.py`](scripts/load_csv_to_db.py)
- **Database**: `marketing_data`
- **Table**: `boxoffice`

#### Table Schema
```sql
CREATE TABLE boxoffice (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    weekend_gross NUMERIC,
    total_gross NUMERIC,
    weeks INTEGER
);
```

### Steps to Load Data
1. **Create the Database**:
   ```sql
   CREATE DATABASE marketing_data;
   ```
2. **Create the Table**:
    ```sql
    CREATE TABLE boxoffice (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    weekend_gross NUMERIC,
    total_gross NUMERIC,
    weeks INTEGER
    );
    ```
### Run the Script
The script [`load_csv_to_db.py`](scripts/load_csv_to_db.py) reads the CSV file, cleans the data, and inserts it into the `boxoffice` table.

- **Table data**: `boxoffice`  
  ![boxoffice](screenshots/postgresql_db_box_office_table_after_insert_from_csv_file.png)

### 3. **Data Processing & Transformation**
- **Objective**: Clean and transform the data for analysis.
- **Tools Used**: Python (pandas), SQL
- **Script**: [`data_processing_panda_postgresql.py`](scripts/data_processing_panda_postgresql.py)
- **Output**: A new table, `boxoffice_transformed`, which contains processed and transformed data.
- **Tasks**:
  - Remove unnecessary columns.
  - Handle missing or invalid data.
  - Convert data types (e.g., strings to numeric).
### Run the Script
The script [`data_processing_panda_postgresql.py`](scripts/data_processing_panda_postgresql.py) process and transform data into the `boxoffice_transformed` table.

- **Table data**: `boxoffice_transformed`  
  ![boxoffice_transformed](screenshots/postgresql_db_box_office_table_after_transformation.png)


### 4. **Data Visualization** 
- **Objective**: Create visualizations to analyze the data.
- **Tools Used**: Python (matplotlib, seaborn), Looker Studio, Tableau.
- **Tasks**:
	- Create charts (e.g., bar charts, line charts) to visualize box office performance.
	- Analyze trends (e.g., top-performing movies, revenue over time).

### 5. **Automation & Deployment**  
- **Objective**: Automate the pipeline and deploy the dashboard.
- **Tools Used**: Apache Airflow, Cron, Flask/Dash, Streamlit.
- **Tasks**:
	- Schedule the pipeline to run daily/weekly.
	- Deploy the dashboard using Streamlit or Flask/Dash.

### Folder Structure ###
```
marketing-data-pipeline/
├── data/
│   ├── raw/                 # Raw scraped data
│   │   └── boxoffice_data.csv
├── scripts/
│   ├── web_scraping.py       # Script for web scraping
│   ├── load_csv_to_db.py     # Script to load data into PostgreSQL
│   ├── data_processing_panda_postgresql.py  # Data transformation script
├── dashboard/                # Visualization components
├── screenshots/              # Screenshots of database & reports
├── README.md                 # Documentation

```
## How to Run the Project

### Prerequisites
- **Python**: Install Python 3.x.
- **PostgreSQL**: Install PostgreSQL and create a database (`marketing_data`).
- **Libraries**: Install the required Python libraries:
  ```bash
  pip install psycopg2 selenium beautifulsoup4 pandas
  ```
## Steps

### 1. Web Scraping
Run the `web_scraping.py` script to collect box office data:
```bash
python scripts/web_scraping.py
```
The output will be saved as boxoffice_data.csv in the data/raw/ folder.

### 2. Load Data into PostgreSQL
Run the `load_csv_to_db.py` script to load the CSV data into PostgreSQL:
```bash
  python scripts/load_csv_to_db.py
```
### 3. Verify the Data
Connect to the marketing_data database and query the boxoffice table:
```sql
  SELECT * FROM boxoffice;
```


