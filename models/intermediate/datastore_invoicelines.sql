{{
    config(
        materialized="incremental",
        unique_key="invoiceline_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('stg_invoicelines') }}
{% endset %}

with
    invoices as (
        select
            -- key
            {{ dbt_utils.generate_surrogate_key(["invoicelineid", "invoiceid"]) }} as invoiceline_key,
            -- attributes
            invoicelineid,
            {{ dbt_utils.generate_surrogate_key(["invoiceid"]) }} as invoice_f_key,
            invoiceid,
            stockitemid,
            description,
            packagetypeid,
            quantity,
            unitprice,
            quantity * unitprice as amount,
            taxrate,
            taxamount,
            amount - taxamount as amount_after_tax,
            lineprofit,
            extendedprice,
            lasteditedby,
            lasteditedwhen,
            triggertime
        from {{ ref("stg_invoicelines") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    )
select *
from invoices
