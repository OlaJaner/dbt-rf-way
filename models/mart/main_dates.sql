{% set mindate %}
    select min(invoicedate) from {{ ref('datastore_invoices') }}
{% endset %}

with date_spine as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('" + mindate + "' as date)",
    end_date="cast('2020-01-01' as date)"
   )
}}

)

select * from date_spine