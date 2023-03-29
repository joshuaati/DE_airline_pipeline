
select 
{{ dbt_utils.surrogate_key(['TailNum', 'FlightNum']) }} as FlightId,
cast(concat(`Year`, '-', `Month`, '-', `DayofMonth`) as date) as FlightDate,
*
from {{source('staging', 'airline_trips')}}