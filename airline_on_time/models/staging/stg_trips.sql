SELECT
-- Identifiers 
{{ dbt_utils.surrogate_key(['TailNum', 'FlightNum']) }} AS FlightId,

-- Date
CAST(concat(`Year`, '-', `Month`, '-', `DayofMonth`) AS date) AS FlightDate,
CAST(Year AS INTEGER) AS Year,
CAST(Month AS INTEGER) AS Month,
CAST(DayofMonth AS INTEGER) AS Day,
{{dow('DayOfWeek')}} AS DayOfWeek,

-- Time
{{ format_time('DepTime') }} AS DepartureTime,
{{ format_time('CRSDepTime') }} AS ScheduledDepartureTime,
{{ format_time('ArrTime') }} AS ArrivalTime,
{{ format_time('CRSArrTime') }} AS ScheduledArrivalTime,

-- Flight Info
UniqueCarrier,
TailNum,
Origin,
Dest AS Destination,
CAST(FlightNum AS INTEGER) AS FlightNum,
CAST(ActualElapsedTime AS INTEGER) AS ActualElapsedTime,
CAST(CRSElapsedTime AS INTEGER) AS ScheduledElapsedTime,
CAST(AirTime AS INTEGER) AS AirTime,
CAST(ArrDelay AS INTEGER) AS ArrivalDelay,
CAST(DepDelay AS INTEGER) AS DepartureDelay,
CAST(Distance AS INTEGER) AS Distance, 
CAST(TaxiIn AS INTEGER) AS TaxiIn, 
CAST(TaxiOut AS INTEGER) AS TaxiOut,

-- Cancellation 
{{yes_no('Cancelled')}} AS Cancelled,
CancellationCode, 
{{cancel('CancellationCode')}}AS CancellationReason, 
{{yes_no('Diverted')}} AS Diverted,
CAST(CarrierDelay AS INTEGER) AS CarrierDelay,
CAST(WeatherDelay AS INTEGER) AS WeatherDelay,
CAST(NASDelay AS INTEGER) AS NASDelay,
CAST(SecurityDelay AS INTEGER) AS SecurityDelay,
CAST(LateAircraftDelay AS INTEGER) AS LateAircraftDelay

FROM {{source('staging', 'airline_trips')}}