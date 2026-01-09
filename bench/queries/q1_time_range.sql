SELECT *
FROM trips
WHERE pickup_datetime >= parseDateTime64BestEffort('{START}', 6)
  AND pickup_datetime <  parseDateTime64BestEffort('{END}', 6);
