{{
    config(
        materialized="incremental",
        unique_key="customer_key",
        incremental_strategy="merge",
    )
}}

-- Find the latest data that should be run
{% set max_trigger_time %}
    select max(TriggerTime) from {{ ref('datastore_customers') }}
{% endset %}

with
    customers as (
        select
            customer_key,
            -- attributes
            customerid,
            customername,
            billtocustomerid,
            customercategoryid,
            case
                when buyinggroupid = 'NULL' then null else buyinggroupid
            end as buyinggroupid,
            -- primarycontactpersonid,
            -- alternatecontactpersonid,
            deliverymethodid,
            deliverycityid,
            postalcityid,
            -- creditlimit,
            -- accountopeneddate,
            -- standarddiscountpercentage,
            -- isstatementsent,
            -- isoncredithold,
            -- paymentdays,
            -- phonenumber,
            -- faxnumber,
            -- deliveryrun,
            -- runposition,
            websiteurl,
            -- deliveryaddressline1,
            -- deliveryaddressline2,
            deliverypostalcode,
            deliverylocation,
            -- postaladdressline1,
            -- postaladdressline2,
            postalpostalcode,
            -- lasteditedby,
            -- validfrom,
            -- validto,
            triggertime
        from {{ ref("datastore_customers") }}
        {% if is_incremental() %}
            where triggertime = ({{ max_trigger_time }})
        {% endif %}
    ),
    customercategories as (select * from {{ ref("datastore_customercategories") }}),
    buyinggroups as (select * from {{ ref("datastore_buyinggroups") }}),
    joined as (

        select c.*, cc.customercategoryname, bg.buyinggroupname
        from customers as c
        left join
            customercategories as cc on cc.customercategoryid = c.customercategoryid
        left join buyinggroups as bg on bg.buyinggroupid = c.buyinggroupid
    )

select *
from joined
