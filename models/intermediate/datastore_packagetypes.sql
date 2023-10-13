{{
    config(
        materialized="incremental",
        unique_key="packagetype_key",
        incremental_strategy="merge",
    )
}}

with
    packagetypes as (
        select
            dbt_scd_id as packagetype_key,
            packagetypeid,
            packagetypename,
            lasteditedby,
            updateat,
            triggertime,
            dbt_updated_at,
            dbt_valid_from,
            coalesce(dbt_valid_to, '9999-12-31 23:59:59.9999999') as dbt_valid_to
        from {{ ref("snap_packagetypes") }}
    )
select *
from packagetypes
