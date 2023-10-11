{{
    config(
        materialized="incremental",
        incremental_strategy="append",
    )
}}
-- always appends new data
select
    *,
    {{ trigger_time_stockholm_time_zone()}} as TriggerTime --call macro
{% if is_incremental() %}
    from {{ source('rf_way', 'invoicelines_incre') }}
{% else %}
    from {{ source('rf_way', 'invoicelines') }}
{% endif %}