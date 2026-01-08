{% macro decimal_to_numeric(units_col, scale_col) %}
  ({{ units_col }} * power(10, -1 * {{ scale_col }}))
{% endmacro %}
