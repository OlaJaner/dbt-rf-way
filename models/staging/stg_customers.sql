{{
    config(
        materialized="incremental",
        incremental_strategy="append",
    )
}}
-- always appends new data
-- ci_cd_test_2
select
    *,
    {{ trigger_time_stockholm_time_zone()}} as TriggerTime --call macro
from {{ source('rf_way', 'customers') }}