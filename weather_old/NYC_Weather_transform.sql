SELECT
  ww.Date,
  ww.Weather,
  ww.Precipitation AS precipitation,
  TIMESTAMP(ww.Date) AS weather_date_ts
FROM {{sdk_rc_x}} AS ww
WHERE LENGTH(TRIM(ww.Precipitation)) !=0