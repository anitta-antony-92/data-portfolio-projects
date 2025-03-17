SELECT
  COUNT(*) AS total_rows,
  COUNTIF(column_name IS NULL) AS missing_values
FROM
  `la-crime-analysis.la_crime_dataset.crime_data`
GROUP BY
  column_name;