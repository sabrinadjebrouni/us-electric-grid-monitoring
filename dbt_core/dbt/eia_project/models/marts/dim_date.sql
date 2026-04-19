with date_series as (
    select 
        full_date
    from unnest(generate_date_array('2019-01-01', '2030-12-31', interval 1 day)) as full_date
),

transformations as (
    select
        cast(format_date('%Y%m%d', full_date) as integer) as date_key,
        full_date,
        format_date('%Y/%m/%d', full_date) as date_name,
        extract(dayofweek from full_date) as day_of_week,
        format_date('%A', full_date) as day_name_of_week,
        extract(day from full_date) as day_of_month,
        extract(dayofyear from full_date) as day_of_year,
        case 
            when extract(dayofweek from full_date) in (1, 7) then 'Weekend' 
            else 'Weekday' 
        end as weekday_weekend,
        extract(isoweek from full_date) as week_of_year,
        format_date('%B', full_date) as month_name,
        extract(month from full_date) as month_of_year,
        case 
            when full_date = last_day(full_date, month) then 'Y' 
            else 'N' 
        end as is_last_day_of_month,
        extract(quarter from full_date) as calendar_quarter,
        extract(year from full_date) as calendar_year,
        format_date('%Y-%m', full_date) as calendar_year_month,
        format_date('%YQ%Q', full_date) as calendar_year_qtr,
        extract(month from date_add(full_date, interval 3 month)) as fiscal_month_of_year,
        extract(quarter from date_add(full_date, interval 3 month)) as fiscal_quarter,
        extract(year from date_add(full_date, interval 3 month)) as fiscal_year,
        format_date('%Y-%m', date_add(full_date, interval 3 month)) as fiscal_year_month,
        format_date('%YQ%Q', date_add(full_date, interval 3 month)) as fiscal_year_qtr

    from date_series
)

select

    cast(date_key as integer) as date_id, --PK format YYYYMMDD
    cast(full_date as date) as full_date,
    cast(date_name as string) as date_name,
    cast(day_of_week as integer) as day_of_week,
    cast(day_name_of_week as string) as day_name_of_week,
    cast(day_of_month as integer) as day_of_month,
    cast(day_of_year as integer) as day_of_year,
    cast(weekday_weekend as string) as weekday_weekend,
    cast(week_of_year as integer) as week_of_year,
    cast(month_name as string) as month_name,
    cast(month_of_year as integer) as month_of_year,
    cast(is_last_day_of_month as string) as is_last_day_of_month,
    cast(calendar_quarter as integer) as calendar_quarter,
    cast(calendar_year as integer) as calendar_year,
    cast(calendar_year_month as string) as calendar_year_month,
    cast(calendar_year_qtr as string) as calendar_year_quarter,
    cast(fiscal_month_of_year as integer) as fiscal_month_of_year,
    cast(fiscal_quarter as integer) as fiscal_quarter,
    cast(fiscal_year as integer) as fiscal_year,
    cast(fiscal_year_month as string) as fiscal_year_month,
    cast(fiscal_year_qtr as string) as fiscal_year_quarter

from transformations