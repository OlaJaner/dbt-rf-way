{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    -- if we are in prod
        -- then we just want customer_schema_name
    
    --{% elif target.name in ['prod', 'test']%}
    {% elif env_var('DBT_MY_ENV') in ['prod', 'test']%}

        {{custom_schema_name | trim }}
    
    {% elif env_var('DBT_MY_ENV') in ['default']%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}