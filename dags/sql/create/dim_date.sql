CREATE TABLE IF NOT EXISTS dim_date AS (
 SELECT 
    Date( dates.d ) AS jour,
    to_char(dates.d, 'MM') AS mois,
    to_char(dates.d, 'YYYY') AS annee,
    to_char(dates.d, 'Day') AS jour_de_la_semaine,
    to_char(dates.d, 'D') AS numero_jour_semaine,
    EXTRACT(WEEK FROM Date( dates.d ) ) AS week_num,
    CASE WHEN extract('ISODOW' from dates.d) IN (6, 7) THEN 0 ELSE 1 END AS is_week_day
FROM (
    SELECT generate_series('2018-01-01'::date, '2025-12-31'::date, '1 day'::interval) AS d
) dates

);

CREATE INDEX if not exists dim_date_index
ON dim_date (jour,mois,annee);