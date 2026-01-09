SELECT date_trunc('hour', pickup_datetime) AS hour, COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= parseDateTime64BestEffort('{START}', 6)
  AND pickup_datetime <  parseDateTime64BestEffort('{END}', 6)
GROUP BY hour
ORDER BY hour;
