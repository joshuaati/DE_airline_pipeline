
WITH trip_data AS 
(
    SELECT *
    FROM {{ref('stg_trips')}}
),

airports AS 
(
    SELECT * 
    FROM {{ref('dim_airports')}}
),

carriers AS 
(
    SELECT * 
    FROM {{ref('dim_carriers')}}
),

plane AS 
(
    SELECT * 
    FROM {{ref('dim_plane-data')}}
)

SELECT 
    trip_data.FlightId,
    trip_data.FlightDate,
    trip_data.Year,
    trip_data.Month,
    trip_data.Day,
    trip_data.DayOfWeek,

    trip_data.DepartureTime,
    trip_data.ScheduledDepartureTime,
    trip_data.Origin,
    origin_airports.airport AS OriginAirport,

    trip_data.ArrivalTime,
    trip_data.ScheduledArrivalTime,
    trip_data.Destination,
    dest_airports.airport AS DestinationAirport,


    trip_data.FlightNum,
    trip_data.UniqueCarrier,
    carriers.Description AS CarrierDescription,
    
    COALESCE(CONCAT(plane.manufacturer, " | ", plane.model), 'No Information') AS Plane,
    trip_data.TailNum,

    trip_data.ActualElapsedTime,
    trip_data.ScheduledElapsedTime,
    trip_data.TaxiIn,
    trip_data.TaxiOut,

    trip_data.AirTime,
    trip_data.Distance,

    trip_data.ArrivalDelay,
    trip_data.DepartureDelay,
    trip_data.CarrierDelay,
    trip_data.WeatherDelay,
    trip_data.NASDelay,
    trip_data.SecurityDelay,
    trip_data.LateAircraftDelay,


    trip_data.Diverted,
    trip_data.Cancelled,
    trip_data.CancellationCode,
    trip_data.CancellationReason





FROM trip_data 
LEFT JOIN airports as origin_airports
ON trip_data.Origin = origin_airports.iata
LEFT JOIN airports as dest_airports
ON trip_data.Destination = dest_airports.iata
LEFT JOIN carriers
ON trip_data.UniqueCarrier = carriers.Code
LEFT JOIN plane
ON trip_data.TailNum = plane.tailnum