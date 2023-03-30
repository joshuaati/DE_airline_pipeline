
{% macro format_time(column_name) %}
    CASE
        WHEN CAST({{ column_name }} AS INT64) > 2359 THEN FORMAT_TIME('%H:%M', TIME(0, 0, 0))
        ELSE FORMAT_TIME('%H:%M', 
            parse_time(
                '%H:%M', 
                CONCAT(
                    SUBSTR(CAST(FORMAT('%04.0f', CAST({{ column_name }} AS FLOAT64)) AS STRING), 1, 2), 
                    ':', 
                    SUBSTR(CAST(FORMAT('%04.0f', CAST({{ column_name }} AS FLOAT64)) AS STRING), 3, 2)
                    )
                )
            )
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