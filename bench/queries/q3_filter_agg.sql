SELECT COUNT(*) AS trip_count, SUM(base_passenger_fare) AS fare
FROM trips
WHERE pickup_datetime >= '{START}' AND pickup_datetime < '{END}'
  AND trip_miles > {MIN_MILES};
