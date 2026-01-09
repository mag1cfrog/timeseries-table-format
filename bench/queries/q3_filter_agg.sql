SELECT COUNT(*) AS trip_count, SUM(base_passenger_fare) AS fare
FROM trips
WHERE pickup_datetime >= parseDateTime64BestEffort('{START}', 6)
  AND pickup_datetime <  parseDateTime64BestEffort('{END}', 6)
  AND trip_miles > {MIN_MILES};
