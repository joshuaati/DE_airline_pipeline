{% macro format_time(column_name) %}
    parse_time(
      '%H:%M', 
      CONCAT(
        SUBSTR(CAST(FORMAT('%04.0f', {{ column_name }}) AS STRING), 1, 2), 
        ':', 
        SUBSTR(CAST(FORMAT('%04.0f', {{ column_name }}) AS STRING), 3, 2))
      )
{% endmacro %}


{% macro dow(column_name) %}
    CASE{{ column_name }} 
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
      WHEN 7 THEN 'Sunday'
    END
{% endmacro %}