{{
    config(
        materialized="incremental",
        unique_key="invoice_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('stg_invoices') }}
{% endset %}

with
    invoices as (
        select
            -- key
            {{ dbt_utils.generate_surrogate_key(["invoiceid"]) }} as invoice_key,
            -- attributes
            invoiceid,
            customerid,
            billtocustomerid,
            orderid,
            deliverymethodid,
            contactpersonid,
            accountspersonid,
            salespersonpersonid,
            packedbypersonid,
            invoicedate,
            customerpurchaseordernumber,
            iscreditnote,
            creditnotereason,
            comments,
            deliveryinstructions,
            internalcomments,
            totaldryitems,
            totalchilleritems,
            deliveryrun,
            runposition,
            returneddeliverydata,
            confirmeddeliverytime,
            confirmedreceivedby,
            lasteditedby,
            lasteditedwhen,
            triggertime
        from {{ ref("stg_invoices") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    )
select *
from invoices
