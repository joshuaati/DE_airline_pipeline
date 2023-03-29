{% macro format_time(column_name) %}
    parse_time(
      '%H:%M', 
      CONCAT(
        SUBSTR(CAST(FORMAT('%04.0f', {{ column_name }}) AS STRING), 1, 2), 
        ':', 
        SUBSTR(CAST(FORMAT('%04.0f', {{ column_name }}) AS STRING), 3, 2))
      )
{% endmacro %}