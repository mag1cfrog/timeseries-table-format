SELECT "PULocationID", COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= parseDateTime64BestEffort('{START}', 6)
  AND pickup_datetime <  parseDateTime64BestEffort('{END}', 6)
GROUP BY "PULocationID";
