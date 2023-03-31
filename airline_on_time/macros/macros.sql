
{% macro format_time(column_name) %}
    CASE
        WHEN CAST({{ column_name }} AS INT64) BETWEEN 0 AND 2359
        AND CAST(SUBSTR(CAST({{ column_name }} AS STRING), 1, 2) AS INT64) BETWEEN 0 AND 23
        AND CAST(SUBSTR(CAST({{ column_name }} AS STRING), -2) AS INT64) BETWEEN 0 AND 59
        THEN FORMAT_TIME('%H:%M', 
            parse_time(
                '%H:%M', 
                CONCAT(
                    SUBSTR(CAST(FORMAT('%04.0f', CAST({{ column_name }} AS FLOAT64)) AS STRING), 1, 2), 
                    ':', 
                    SUBSTR(CAST(FORMAT('%04.0f', CAST({{ column_name }} AS FLOAT64)) AS STRING), 3, 2)
                    )
                )
            )
        ELSE NULL
    END
{% endmacro %}


{% macro dow(column_name) %}
    CASE {{ column_name }} 
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
      WHEN 7 THEN 'Sunday'
    END
{% endmacro %}


{% macro month_full(column_name) %}
    CASE {{ column_name }} 
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
    END
{% endmacro %}


{% macro yes_no(column_name) %}
    CASE {{ column_name }} 
      WHEN 0 THEN 'No'
      WHEN 1 THEN 'Yes'
    END
{% endmacro %}


{% macro cancel(column_name) %}
    CASE {{ column_name }} 
      WHEN 'A' THEN 'Carrier'
      WHEN 'B' THEN 'Weather'
      WHEN 'C' THEN 'NAS'
      WHEN 'D' THEN 'Security'
    END
{% endmacro %}