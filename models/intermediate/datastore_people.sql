{{
    config(
        materialized="incremental",
        unique_key="person_key",
        incremental_strategy="merge",
    )
}}

with
    packagetypes as (
        select
            dbt_scd_id as person_key,
            personid,
            fullname,
            preferredname,
            searchname,
            ispermittedtologon,
            isemployee,
            issalesperson,
            phonenumber,
            faxnumber,
            emailaddress,
            updatedat,
            lasteditedby,
            triggertime,
            dbt_updated_at,
            dbt_valid_from,
            coalesce(dbt_valid_to, '9999-12-31 23:59:59.9999999') as dbt_valid_to
        from {{ ref("snap_people") }}
    )
select *
from packagetypes
