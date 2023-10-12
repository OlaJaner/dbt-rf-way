{{
    config(
        materialized="incremental",
        unique_key="customercategory_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('stg_customercategories') }}
{% endset %}

with
    customercategories as (
        select
            -- key
            {{ dbt_utils.generate_surrogate_key(["customercategoryid"]) }} as customercategory_key,
            -- attributes
            customercategoryid,
            customercategoryname,
            lasteditedby,
            validfrom,
            validto,
            triggertime
        from {{ ref("stg_customercategories") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    )
select *
from customercategories
