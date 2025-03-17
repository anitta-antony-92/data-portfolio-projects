select * from `la-crime-analysis.la_crime_dataset.crime_data`
where date_occ = '2022-09-11 00:00:00 UTC'
and crm_cd = 110;


select distinct crm_cd_desc from `la-crime-analysis.la_crime_dataset.crime_data` ;

SELECT * FROM `la-crime-analysis.la_crime_dataset.crime_data` limit 10;

SELECT
  dr_no,
  date_rptd,
  date_occ,
  time_occ,
  area,
  crm_cd,
  COUNT(*) AS duplicate_count
FROM
  `la-crime-analysis.la_crime_dataset.crime_data`
GROUP BY
  dr_no, date_rptd, date_occ, time_occ, area, crm_cd
HAVING
  COUNT(*) > 1;


select distinct vict_descent from `la-crime-analysis.la_crime_dataset.crime_data` ;

select vict_age, count(*) as count from `la-crime-analysis.la_crime_dataset.crime_data`
group by vict_age ;

select * from `la-crime-analysis.la_crime_dataset.crime_data` where vict_age = 120;

-- 232012240


SELECT
  crm_cd,
  COUNT(*) AS crime_count
FROM
  `la-crime-analysis.la_crime_dataset.crime_data`
GROUP BY
  crm_cd
ORDER BY
  crm_cd;




SELECT
  date_occ,
  time_occ,
  location,
  crm_cd,
  vict_age,
  vict_sex,
  vict_descent,
  COUNT(*) AS count
FROM
  `la-crime-analysis.la_crime_dataset.crime_data`
GROUP BY
  date_occ,
  time_occ,
  location,
  crm_cd,
  vict_age,
  vict_sex,
  vict_descent
HAVING
  COUNT(*) > 1;




SELECT
  date_occ,
  time_occ,
  location,
  crm_cd,
  vict_age,
  vict_sex,
  vict_descent,
  premis_cd,
  weapon_used_cd,
  status,
  lat,
  lon,
  hour_of_day,
  day_of_week,
  month,
  COUNT(*) AS count
FROM
  `la-crime-analysis.la_crime_dataset.crime_data`
GROUP BY
  date_occ,
  time_occ,
  location,
  crm_cd,
  vict_age,
  vict_sex,
  vict_descent,
  premis_cd,
  weapon_used_cd,
  status,
  lat,
  lon,
  hour_of_day,
  day_of_week,
  month
HAVING
  COUNT(*) > 1;