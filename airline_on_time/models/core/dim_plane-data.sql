SELECT
    tailnum,
    type,
    CASE 
        WHEN manufacturer = 'MCDONNELL DOUGLAS AIRCRAFT CO' THEN 'MCDONNELL DOUGLAS'
        WHEN manufacturer = 'MCDONNELL DOUGLAS CORPORATION' THEN 'MCDONNELL DOUGLAS'
        WHEN manufacturer = 'AIRBUS INDUSTRIE' THEN 'AIRBUS'
        WHEN manufacturer = 'BOMBARDIER INC' THEN 'MCDONNELL DOUGLAS'
        ELSE manufacturer
    END AS manufacturer,
    CASE 
        WHEN issue_date = 'None' THEN NULL
        ELSE PARSE_DATE('%m/%d/%Y', issue_date) 
    END AS issue_date,
    model,
    status,
    aircraft_type,
    engine_type,
    CASE 
        WHEN year = 'None' THEN NULL
        ELSE CAST(year as INTEGER) 
    END AS year
FROM {{ref('plane-data')}}