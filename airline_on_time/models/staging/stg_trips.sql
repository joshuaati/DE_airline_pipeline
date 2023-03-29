SELECT
-- Identifiers 
{{ dbt_utils.surrogate_key(['TailNum', 'FlightNum']) }} AS FlightId,

-- Date
CAST(concat(`Year`, '-', `Month`, '-', `DayofMonth`) AS date) AS FlightDate,
CAST(Year AS INTEGER) AS Year
CAST(Month AS INTEGER) AS Month
CAST(DayofMonth AS INTEGER) AS Day
{{dow('DayOfWeek')}} AS DayOfWeek

-- Time
{{ format_time('DepTime') }} AS DepartureTime,
{{ format_time('CRSDepTime') }} AS SheduledDepartureTime,
{{ format_time('ArrTime') }} AS ArrivalTime,
{{ format_time('CRSArrTime') }} AS SheduledArrivalTime,

UniqueCarrier,

CAST(FlightNum AS INTEGER) AS FlightNum,
TailNum,
CAST(ActualElapsedTime AS INTEGER) AS ActualElapsedTime
CAST(CRSElapsedTime AS INTEGER) AS SheduledElapsedTime
CAST(CRSElapsedTime AS INTEGER) AS SheduledElapsedTime

FROM {{source('staging', 'airline_trips')}}