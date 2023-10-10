{{
    config(
        materialized="incremental",
        unique_key="customer_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time_invoices %}
    select max(TriggerTime) from {{ ref('datastore_invoices') }}
{% endset %}

{% set max_trigger_time_invoicelines %}
    select max(TriggerTime) from {{ ref('datastore_invoicelines') }}
{% endset %}

with
    invoices as (
        select
            *
        from {{ ref("datastore_invoices") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time_invoices }})
        {% endif %}
    ),
    invoicelines as (
        select
            *
        from {{ ref("datastore_invoicelines") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time_invoicelines }})
        {% endif %}
    )

select
    i.invoice_key,
    i.invoiceid,
    i.customerid,
    i.orderid,
    il.invoicelineid,
    il.stockitemid,
    il.description,
    il.quantity,
    il.unitprice,
    il.taxrate,
    il.taxamount,
    il.lineprofit,
    il.extendedprice
from invoices as i
left join invoicelines as il on il.invoice_f_key = i.invoice_key
