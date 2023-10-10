{%- macro limit_data_in_dev(column_name, dev_days_of_data=3, optional_date='2018-02-01') -%}
{% if target.name == 'default' %}
where {{column_name}} >= '{{ optional_date }}'-- dateadd('day', - {{dev_days_of_data}}, current_timestamp)-- '2018-02-01'
order by {{column_name}}
{% endif %}
{%- endmacro -%}