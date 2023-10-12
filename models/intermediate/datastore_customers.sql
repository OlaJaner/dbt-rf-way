{{
    config(
        materialized="incremental",
        unique_key="customer_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('stg_customers') }}
{% endset %}

with
    customers as (
        select
            -- key
            {{ dbt_utils.generate_surrogate_key(["customerid"]) }} as customer_key,
            -- attributes
            customerid,
            customername,
            billtocustomerid,
            customercategoryid,
            buyinggroupid,
            primarycontactpersonid,
            alternatecontactpersonid,
            deliverymethodid,
            deliverycityid,
            postalcityid,
            creditlimit,
            accountopeneddate,
            standarddiscountpercentage,
            isstatementsent,
            isoncredithold,
            paymentdays,
            phonenumber,
            faxnumber,
            deliveryrun,
            runposition,
            websiteurl,
            deliveryaddressline1,
            deliveryaddressline2,
            deliverypostalcode,
            deliverylocation,
            postaladdressline1,
            postaladdressline2,
            postalpostalcode,
            lasteditedby,
            validfrom,
            validto,
            triggertime
        from {{ ref("stg_customers") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    )
select *
from customers
