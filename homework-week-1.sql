/* Question 3 */
SELECT COUNT(1)
	FROM public.green_taxi_trips
	WHERE DATE(lpep_pickup_datetime) = '2019-01-15'  
	AND DATE(lpep_dropoff_datetime) = '2019-01-15';
  
  /* Question 4 */
  SELECT lpep_pickup_datetime, trip_distance
	FROM public.green_taxi_trips
	WHERE trip_distance = (SELECT MAX(trip_distance) FROM public.green_taxi_trips);
  
  /* Question 5 */
  SELECT COUNT(1)
	FROM public.green_taxi_trips
	WHERE DATE(lpep_pickup_datetime) = '2019-01-01' 
	AND passenger_count = 2;
    /* Second Part */
  SELECT COUNT(1)
	FROM public.green_taxi_trips
	WHERE DATE(lpep_pickup_datetime) = '2019-01-01' 
	AND passenger_count = 3;
  
  /* Question 6 */
  SELECT index, "LocationID", "Borough", "Zone", service_zone
	FROM public.green_taxi_locations
	WHERE "Zone" = 'Astoria'
	LIMIT 10;
  
  /* Found location ID for Astoria to be 7 */
  
  SELECT "Zone", "DOLocationID", tip_amount
  FROM public.green_taxi_locations as l
  JOIN public.green_taxi_trips as r
  ON l."LocationID" = r."PULocationID"
  WHERE r."PULocationID" = 7
  ORDER BY tip_amount DESC;
  
  /* Found Maximum tip amount to be in DOLocation = 146 */
  
  SELECT "Zone"
  FROM public.green_taxi_locations
  WHERE "LocationID" = 146;
  
/* Finally looked up the Zone */
