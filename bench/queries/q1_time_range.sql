SELECT *
FROM trips
WHERE pickup_datetime >= '{START}' AND pickup_datetime < '{END}';
