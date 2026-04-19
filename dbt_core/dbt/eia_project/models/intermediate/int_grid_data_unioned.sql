with generation as (
    select
        period,
        respondent as ba_code,
        fueltype as type_code,
        "Unknown" as related_ba,
        value,
        value_units
    from {{ source('hourly_grid_data', 'fuel_type') }}
    where respondent not in ('CAL','CAR','CENT','FLA','MIDA','MIDW','NE','NW','NY','SE','SW','TEN','TEX','US48')
),

interchange as (
    select
        period,
        fromba as ba_code,
        "FLOW" as type_code,
        toba as related_ba,
        value,
        value_units
    from {{ source('hourly_grid_data', 'interchange') }}
    where fromba not in ('CAL','CAR','CENT','FLA','MIDA','MIDW','NE','NW','NY','SE','SW','TEN','TEX','US48')
),

demand as (
    select
        period,
        respondent as ba_code,
        type as type_code,
        "Unknown" as related_ba,
        value,
        value_units
    from {{ source('hourly_grid_data', 'region') }}
    where type not in ('TI', 'NG')
    and respondent not in ('CAL','CAR','CENT','FLA','MIDA','MIDW','NE','NW','NY','SE','SW','TEN','TEX','US48')
)

select * from generation
union all
select * from interchange
union all
select * from demand