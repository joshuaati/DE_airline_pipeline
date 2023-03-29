WITH source_data AS (
    SELECT *, 
    CAST(FORMAT('%04.0f', DepTime) AS STRING) AS formatted_DepTime
    FROM {{source('staging', 'airline_trips')}}
),
formatted_data AS (
    SELECT *,
    parse_time('%H:%M', CONCAT(SUBSTR(formatted_DepTime, 1, 2), ':', SUBSTR(formatted_DepTime, 3, 2))) AS formatted_time
    FROM source_data
)
SELECT * FROM formatted_data