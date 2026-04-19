with ba_seed as (
    select * from {{ ref('balancing_authorities') }}
)

select

    
    cast(`BA Id` as integer) as ba_id, --PK
    cast(`BA Code` as string) as ba_code,
    cast(`BA Name` as string) as ba_name,
    cast(`Time Zone` as string) as time_zone,
    cast(`Region_Country Code` as string) as region_country_code,
    cast(`Region_Country Name` as string) as region_country_name,
    cast(`Generation Only BA` as string) as is_generation_only,
    cast(`Demand by BA Subregion` as string) as has_demand_subregion,
    cast(`US_BA` as string) as is_us_ba,
    cast(`Active BA` as string) as is_active,
    cast(`Activation Date` as date) as activation_date,
    cast(`Retirement Date` as date) as retirement_date,
    cast(LATITUDE as float64) as latitude,
    cast(LONGITUDE as float64) as longitude

from ba_seed

