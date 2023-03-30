
WITH trip_data AS 
(
    SELECT *
    FROM {{ref('stg_trips')}}
),

airports AS 
(
    SELECT * 
    FROM {{ref{'dim_airports'}}}
)

carriers AS 
(
    SELECT * 
    FROM {{ref{'dim_carriers'}}}
)

plane AS 
(
    SELECT * 
    FROM {{ref{'dim_airports'}}}
)

SELECT 