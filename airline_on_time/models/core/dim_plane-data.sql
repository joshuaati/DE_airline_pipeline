SELECT
    tailnum,
    type,
    manufacturer,
    CASE WHEN issue_date = 'None' THEN NULL
    ELSE PARSE_DATE('%m/%d/%Y', issue_date) END AS issue_date,
    model,
    status,
    aircraft_type,
    engine_type,
    CASE WHEN year = 'None' THEN NULL
    ELSE CAST(year as INTEGER) END AS year
FROM {{ref('plane-data')}}