{{
    config(
        materialized="incremental",
        unique_key="invoiceline_key",
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
    ),
    customers as (
        select *
        from {{ ref('datastore_customers') }}
    ),
    packagetypes as (
        select *
        from {{ ref('datastore_packagetypes') }}
    ),
    people as (
        select *
        from {{ ref('datastore_people') }}
    ),
    joined as (
        select
            i.invoice_key,
        --    i.invoiceid,
            --i.customerid,
            c.customer_key,
        --    i.orderid,
            i.invoicedate,
        --    il.invoicelineid,
            il.invoiceline_key,
            il.stockitemid,
            il.description,
        --    il.packagetypeid,
            pt.packagetype_key,
            pc.person_key as contact_person_key,
            ps.person_key as sales_person_key,
            il.quantity,
            il.unitprice,
            il.amount,
            il.taxrate,
            il.taxamount,
            il.amount_after_tax,
            il.lineprofit,
            il.extendedprice,
            --il.lasteditedwhen,
            il.triggertime
        from invoices as i
        left join invoicelines as il on il.invoice_f_key = i.invoice_key
        left join customers as c on c.customerid = i.customerid
        left join packagetypes as pt on pt.packagetypeid = il.packagetypeid and i.InvoiceDate >= pt.dbt_valid_from and i.InvoiceDate < pt.dbt_valid_to
        left join people as pc on pc.personid = i.contactpersonid and i.InvoiceDate >= pc.dbt_valid_from and i.InvoiceDate < pc.dbt_valid_to
        left join people as ps on ps.personid = i.salespersonpersonid and i.InvoiceDate >= ps.dbt_valid_from and i.InvoiceDate < ps.dbt_valid_to
    )

select *
from joined
