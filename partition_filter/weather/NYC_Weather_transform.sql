SELECT
  ww.Date,
  ww.Weather,
  ww.Precipitation AS precipitation,
  TIMESTAMP(ww.Date) AS weather_date_ts
FROM {{NYC_Weather}} AS ww
WHERE LENGTH(TRIM(ww.Precipitation)) !=0