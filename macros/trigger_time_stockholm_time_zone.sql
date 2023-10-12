{%- macro trigger_time_stockholm_time_zone() -%}
CONVERT_TIMEZONE('America/Los_Angeles', 'Europe/Stockholm', CURRENT_TIMESTAMP())
{%- endmacro -%}