{{ 
    config(
    materialized='table',
    partition_by={
      "field": "period",
      "data_type": "timestamp",
      "granularity": "day"
    })
}}

with unioned_data as (
    select * from {{ ref('int_grid_data_unioned') }}
),

transformations as (
    select

        cast(format_timestamp('%Y%m%d', period) as integer) as date_id,
        cast(format_timestamp('%H', period) as integer) as time_id,
        ba_code,
        type_code,
        related_ba,
        value,
        value_units,
        period
    from unioned_data
),

final_join as (
    select

        ba.ba_id,
        t.type_id,
        rba.ba_id as related_ba_id,
        d.date_id,
        tod.time_id,
        u.value,
        u.value_units,
        u.period
        
    from transformations u
    left join {{ ref('dim_ba') }} ba 
        on u.ba_code = ba.ba_code

    left join {{ ref('dim_type') }} t 
        on u.type_code = t.type_code

    left join {{ ref('dim_ba') }} rba 
        on u.related_ba = rba.ba_code

    left join {{ ref('dim_date') }} d
        on u.date_id = d.date_id

    left join {{ ref('dim_time_of_day') }} tod
        on u.time_id = tod.time_id
)

select

    cast(date_id as integer) as date_id, --FK to dim_date
    cast(time_id as integer) as time_id, --FK to dim_time_of_day
    cast(type_id as integer) as type_id, --FK to dim_type
    cast(ba_id as integer) as ba_id, --FK to dim_ba
    cast(related_ba_id as integer) as related_ba_id, --FK to dim_ba, needed for FLOW type

    cast(value as integer) as value,
    cast(value_units as string) as value_units,
    cast(period as timestamp) as period

from final_join