SELECT
    tailnum,
    type,
    manufacturer,
    PARSE_DATE('%m/%d/%Y', issue_date) as issue_date,
    model,
    status,
    aircraft_type,
    engine_type,
    CASE WHEN year = "None" THEN NULL
    ELSE CAST(year as INTEGER) END AS year
FROM {{ref('plane-data')}}
