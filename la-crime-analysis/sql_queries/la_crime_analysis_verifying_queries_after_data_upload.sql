SELECT *
FROM `la_crime_dataset.crime_data`
LIMIT 10;

SELECT *
FROM `la_crime_dataset.crime_data`
WHERE status IS NULL OR date_rptd IS NULL OR date_occ IS NULL;

SELECT premis_cd, weapon_used_cd
FROM `la_crime_dataset.crime_data`
WHERE MOD(premis_cd, 1) != 0 OR MOD(weapon_used_cd, 1) != 0;

SELECT DISTINCT status
FROM `la_crime_dataset.crime_data`;

SELECT COUNT(*)
FROM `la_crime_dataset.crime_data`;
