-- Query 1: Top 5 movies by total gross
SELECT title, total_gross
FROM boxoffice_transformed
ORDER BY total_gross DESC
LIMIT 5;

-- Query 2: Average weekend gross for all movies
SELECT AVG(weekend_gross) AS average_weekend_gross
FROM boxoffice_transformed;

-- Query 3: Summary of average gross per week
SELECT
    MIN(average_gross_per_week) AS min_avg_gross,
    MAX(average_gross_per_week) AS max_avg_gross,
    AVG(average_gross_per_week) AS avg_gross,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY average_gross_per_week) AS median_avg_gross
FROM boxoffice_transformed;

-- Query 4: Weekend gross vs. total gross
SELECT title, weekend_gross, total_gross, weeks
FROM boxoffice_transformed;