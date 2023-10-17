select
   *,
   {{ trigger_time_stockholm_time_zone()}} as TriggerTime --call macro
--load from one table when incremental, otherwise from full
{% if is_incremental() %}
    from {{ source('rf_way', 'invoices_incre') }}
{% else %}
    from {{ source('rf_way', 'invoices') }}
{% endif %}
