# Los Angeles Crime Data Analysis & Prediction

**Author:** Anitta Antony  
**Repository:** [GitHub - LA Crime Analysis](https://github.com/anitta-antony-92/data-portfolio-projects/tree/main/la-crime-analysis)  

---

## 📌 Project Overview
This project analyzes crime trends in Los Angeles using publicly available crime data. The primary objective is to demonstrate expertise in **data engineering, analytics, visualization, and predictive modeling**. The analysis provides insights into crime distribution across time and location, helping identify high-risk areas and trends.

### **Key Features:**
- **Data Collection & Preprocessing**: Extract, clean, and transform raw crime data.
- **Database Integration**: Store and query processed data efficiently.
- **Exploratory Data Analysis (EDA)**: Identify patterns, trends, and correlations.
- **Machine Learning Model**: Predict crime occurrences based on historical data.
- **Visualization & Reporting**: Generate interactive dashboards using **Tableau**.

This project is part of my **data portfolio**, aimed at demonstrating end-to-end data analysis and predictive modeling workflows.

---

## 🛠 Tech Stack
| **Category**         | **Tools & Technologies**          |
|----------------------|---------------------------------|
| **Data Collection**  | Python (Requests, Pandas, Polars)       |
| **Data Storage**    | Google BigQuery (Free Tier)    |
| **Data Processing** | Pandas, Polars, NumPy          |
| **ML & Analytics**  | Scikit-learn          |
| **Visualization**   | Tableau                         |
| **Workflow**        | Jupyter Notebook, Python Scripts |

---

## 📂 Project Structure
```
la-crime-analysis/
├── code_texts/                                    # Folder for project documentation
│   ├── Los_Angeles_Crime_Prediction_ML_Logic_Explanation.docx
│   └── Machine_Learning_Project_Documentation.docx
│   ├── data_schema/                               # Data schema definitions
│   ├── processed_data/                            # Cleaned and processed data
│   │   └── pre_cleaned_crime_data.parquet
│   └── raw_data/                                  # Raw data
│       └── la_crime_data.csv
├── log/                                           # Folder for log files
│   ├── data_cleaning.log
│   ├── la_crime_type_model_training_evaluation.log
│   └── model_training_crime_analysis.log
│   └── old/                                       # Old log files
│       ├── model_training_output1.txt
│       ├── model_training_output2.txt
│       └── model_training_output2_garbage_collection.txt
├── notebooks/                                     # Jupyter notebooks
│   ├── eda.ipynb
│   ├── eda_geospatial.ipynb
│   └── LA_Crime_Analysis_Model_Google_Colab.ipynb
├── output/                                        # Folder for output files
│   ├── sql_bigquery_output.xlsx
│   ├── deployment_model/                          # Model deployment files
│   │   ├── evaluation_metrics.json
│   │   ├── la_crime_type_model.pkl
│   │   ├── model.pkl
│   │   └── model_evaluation_metrics.json
│   └── model_training_output_files/               # Model training output
│       ├── classification_metrics.csv
│       ├── confusion_matrix.csv
│       └── reshaped_confusion_matrix.csv
├── screenshots/                                   # Folder for screenshots
│   ├── data_download.png
│   └── data_upload_to_bigquery.png
├── sql_queries/                                   # SQL query files
│   ├── additional_cleanup_queries.txt
│   ├── EDA_Queries_for_Crime_Data.txt
│   ├── la_crime_analysis_verifying_queries_after_data_upload.sql
│   ├── queries-for-data-cleaning.sql
│   ├── select_queries_for_data_check.sql
│   └── table_alter_update_queries.sql
├── src/                                           # Folder for source code
│   ├── data_preprocessing_before_bigquery.py
│   ├── download_data.py
│   ├── la_crime_model_results_visualization.py
│   ├── la_crime_type_model_training_evaluation.py
│   └── upload_to_bigquery.py
│   └── src_extras/                                # Extra source code files for texting and explanations
│       ├── code_examples.py
│       ├── confusion_matrix_conversion.py
│       ├── data_preprocessing_before_bigquery_optimized_knn.py
│       ├── la_crime_type_model_training.py
│       ├── ml_model_training.py
│       ├── ml_model_training_limit_1.py
│       ├── ml_model_training_limit_2.py
│       ├── ml_model_training_limit_3.py
│       ├── test_bigquery_connection.py
│       ├── upload_to_bigquery_debug_comments.py
│       └── verify_cleaned_data.py
├── visualizations/                                 # Folder for visualizations
│   ├── Crime Prediction Metrics (Precision, Recall, F1).twb
│   └── exploratory_data_analysis_viz/              # EDA visualizations
│       ├── crime_by_day_of_week_barplot.png
│       ├── crime_by_hour_of_day_barplot.png
│       ├── crime_distribution_by_type_barplot.png
│       ├── crime_distribution_by_type_countplot.png
│       ├── crime_distribution_by_type_vertical_20_barplot.png
│       ├── crime_distribution_by_type_Vertical_barplot.png
│       ├── crime_heatmap.html
│       └── monthly_crime_trends.png

```

---

## 📊 Data Workflow
1. **Data Collection**: Crime data is sourced from the [LA Open Data Portal](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/).
2. **Data Preprocessing**: The dataset undergoes cleaning and transformation using Pandas and Polars.
3. **Exploratory Data Analysis**: Identify trends, seasonal patterns, and crime hotspots.
4. **Machine Learning Model**: A predictive model is trained to forecast crime occurrences.
5. **Database Integration**: Processed data is stored in **Google BigQuery** for analysis.
6. **Visualization & Reporting**: Insights are presented via **Tableau dashboards**.

---

## 🔢 Key Insights & Results  
✔️ **Tableau dashboard** provides interactive crime analysis visualization.  

---

## 🚀 How to Run the Project
### **Prerequisites**
- Python 3.x installed
- Virtual environment (recommended)
- Google Cloud BigQuery account (Free Tier)
- Tableau Public installed (for dashboard visualization)

### **Steps to Run**
1. **Clone the Repository**
   ```bash
   git clone https://github.com/anitta-antony-92/data-portfolio-projects.git
   cd data-portfolio-projects/la-crime-analysis
   ```
2. **Fetch Crime Data**
   ```bash
   python src/ingestion/fetch_data.py
   ```
3. **Clean and Transform Data**
   ```bash
   python src/processing/clean_data.py
   python src/processing/transform_data.py
   ```
4. **Store Processed Data in BigQuery** *(Ensure Google Cloud credentials are set up)*
   ```bash
   python src/processing/upload_to_bigquery.py
   ```
5. **Train Machine Learning Model**
   ```bash
   python src/modeling/train_model.py
   ```
6. **Evaluate Model Performance**
   ```bash
   python src/modeling/evaluate_model.py
   ```
7. **Visualize Insights in Tableau**
   - Open Tableau and load the dataset from `data/processed/`
   - Load and interact with the prebuilt Tableau dashboard (`tableau/Crime_Analysis_Dashboard.twbx`)

---

## 📊 Tableau Dashboard
- **Crime Trends & Hotspots:** Interactive visualizations to analyze crime patterns.  


🖥 **[View Interactive Tableau Dashboard](#)** *(To be added)*

---

## 🔗 Additional Resources
- **Dataset Source**: [LA Open Data](https://data.lacity.org/Public-Safety/Crime-Data-from-2020-to-Present/)
- **Google BigQuery Free Tier Guide**: [Google BigQuery](https://cloud.google.com/bigquery/pricing)
- **Tableau Public**: [Tableau Public](https://public.tableau.com/)
- **Apache Airflow Docs**: [Airflow](https://airflow.apache.org/)

---

## 📌 Why This Project Matters
✅ **Demonstrates expertise in data processing & analytics**  
✅ **Showcases hands-on experience with BigQuery, Polars, Pandas, and ML**  
✅ **Presents actionable insights using Tableau dashboards**  
✅ **End-to-end implementation from raw data to insights, with automation using Apache Airflow**  
