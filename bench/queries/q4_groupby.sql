SELECT "PULocationID", COUNT(*) AS trips
FROM trips
WHERE pickup_datetime >= '{START}' AND pickup_datetime < '{END}'
GROUP BY "PULocationID";
