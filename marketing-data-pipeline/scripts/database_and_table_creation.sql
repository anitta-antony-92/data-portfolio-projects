# Create the Database:
   
   CREATE DATABASE marketing_data;
   
# Create the Table:

CREATE TABLE boxoffice (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    weekend_gross NUMERIC,
    total_gross NUMERIC,
    weeks INTEGER
);
    