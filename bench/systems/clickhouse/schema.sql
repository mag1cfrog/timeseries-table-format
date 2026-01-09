CREATE DATABASE IF NOT EXISTS bench;
DROP TABLE IF EXISTS bench.trips;

CREATE TABLE bench.trips
(
  hvfhs_license_num Nullable(String),
  dispatching_base_num Nullable(String),
  originating_base_num Nullable(String),
  request_datetime Nullable(DateTime64(6)),
  on_scene_datetime Nullable(DateTime64(6)),
  pickup_datetime Nullable(DateTime64(6)),
  dropoff_datetime Nullable(DateTime64(6)),
  PULocationID Nullable(Int32),
  DOLocationID Nullable(Int32),
  trip_miles Nullable(Float64),
  trip_time Nullable(Int64),
  base_passenger_fare Nullable(Float64),
  tolls Nullable(Float64),
  bcf Nullable(Float64),
  sales_tax Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64),
  tips Nullable(Float64),
  driver_pay Nullable(Float64),
  shared_request_flag Nullable(String),
  shared_match_flag Nullable(String),
  access_a_ride_flag Nullable(String),
  wav_request_flag Nullable(String),
  wav_match_flag Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_datetime)
SETTINGS allow_nullable_key = 1;
