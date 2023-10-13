{{
    config(
        materialized="incremental",
        unique_key="packagetype_key",
        incremental_strategy="merge",
    )
}}

with packagetypes as (select * from {{ ref("datastore_packagetypes") }})
select *
from packagetypes
