SELECT COUNT(*) AS trip_count, SUM(trip_miles) AS miles
FROM trips
WHERE pickup_datetime >= '{START}' AND pickup_datetime < '{END}';
