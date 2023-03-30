
WITH trip_data AS 
(
SELECT *
FROM {{ref('stg_trips')}}
),

