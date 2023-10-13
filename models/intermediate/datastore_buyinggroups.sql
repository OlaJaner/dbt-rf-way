{{
    config(
        materialized="incremental",
        unique_key="buyinggroup_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('stg_buyinggroups') }}
{% endset %}

with
    buyinggroups as (
        select
            -- key
            {{ dbt_utils.generate_surrogate_key(["buyinggroupid"]) }} as buyinggroup_key,
            -- attributes
            buyinggroupid,
            buyinggroupname,
            lasteditedby,
            validfrom,
            validto,
            triggertime
        from {{ ref("stg_buyinggroups") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    )
select *
from buyinggroups
