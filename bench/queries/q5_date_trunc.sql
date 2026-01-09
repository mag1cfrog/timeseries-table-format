SELECT date_trunc('hour', pickup_datetime) AS hour, COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= '{START}' AND pickup_datetime < '{END}'
GROUP BY hour
ORDER BY hour;
