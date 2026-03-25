-- ============================================================
-- Macro: Convert temperature between scales
-- ============================================================

{% macro celsius_to_fahrenheit(column_name) %}
    ({{ column_name }} * 9.0 / 5.0 + 32)
{% endmacro %}

{% macro fahrenheit_to_celsius(column_name) %}
    (({{ column_name }} - 32) * 5.0 / 9.0)
{% endmacro %}
