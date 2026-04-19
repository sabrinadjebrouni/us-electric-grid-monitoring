with hour_series as (
    select 
        hour_num
    from unnest(generate_array(0, 23)) as hour_num
),

transformations as (
    select

        cast(hour_num as integer) as time_key,
        time(hour_num, 0, 0) as time_actual,
        format_time('%H:%M', time(hour_num, 0, 0)) as time_name_24,
        format_time('%I %p', time(hour_num, 0, 0)) as time_name_12,
        hour_num as hour_24,
        case 
            when hour_num = 0 then 12 
            when hour_num > 12 then hour_num - 12 
            else hour_num 
        end as hour_12,
        case when hour_num < 12 then 'AM' else 'PM' end as am_pm,
        case 
            when hour_num between 5 and 11 then 'Morning'
            when hour_num between 12 and 16 then 'Afternoon'
            when hour_num between 17 and 20 then 'Evening'
            else 'Night'
        end as day_part,
        case 
            when hour_num between 7 and 22 then 'Peak'
            else 'Off-Peak'
        end as peak_period

    from hour_series
)

select

    cast(time_key as integer) as time_id, --PK format H from 0 to 23
    cast(time_actual as time) as time_actual,
    cast(time_name_24 as string) as time_name_24,
    cast(time_name_12 as string) as time_name_12,
    cast(hour_24 as integer) as hour_24,
    cast(hour_12 as integer) as hour_12,
    cast(am_pm as string) as am_pm,
    cast(day_part as string) as day_part,
    cast(peak_period as string) as peak_period

from transformations