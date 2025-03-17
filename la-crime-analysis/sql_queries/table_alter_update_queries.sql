CREATE TABLE `la-crime-analysis.la_crime_dataset.crime_data_backup` AS
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`;

update `la-crime-analysis.la_crime_dataset.crime_data`
set vict_sex = 'Unknown'
where dr_no = 232012240; -- where vict_sex was '-'

update `la-crime-analysis.la_crime_dataset.crime_data`
set vict_sex = 'M'
where vict_sex = 'H'; -- assuming 'H' is 'Hombre' (Spanish for Male)

update `la-crime-analysis.la_crime_dataset.crime_data`
set vict_descent = 'Unknown'
where vict_descent = '-'; -- where vict_descent was '-'

update `la-crime-analysis.la_crime_dataset.crime_data`
set
  vict_age = case 
    when vict_age <=0 then NULL
    when vict_age > 100 then null
    else vict_age
  end
where true; -- updated negative, zero and above 100 values into null

CREATE TABLE `la-crime-analysis.la_crime_dataset.crime_data_clean_with_dup` AS
SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data`;


