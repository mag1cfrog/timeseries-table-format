DROP TABLE IF EXISTS trips;

CREATE TABLE trips (
  hvfhs_license_num TEXT,
  dispatching_base_num TEXT,
  originating_base_num TEXT,
  request_datetime TIMESTAMP,
  on_scene_datetime TIMESTAMP,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  "PULocationID" INT,
  "DOLocationID" INT,
  trip_miles DOUBLE PRECISION,
  trip_time BIGINT,
  base_passenger_fare DOUBLE PRECISION,
  tolls DOUBLE PRECISION,
  bcf DOUBLE PRECISION,
  sales_tax DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION,
  airport_fee DOUBLE PRECISION,
  tips DOUBLE PRECISION,
  driver_pay DOUBLE PRECISION,
  shared_request_flag TEXT,
  shared_match_flag TEXT,
  access_a_ride_flag TEXT,
  wav_request_flag TEXT,
  wav_match_flag TEXT
);
