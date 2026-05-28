{{ config(materialized='table') }}

with date_spine as (

    select
        dateadd(day, seq4(), '2000-01-01'::date) as date_day
    from table(generator(rowcount => 30000))
    -- ~82 years coverage; safely exceeds today

),

filtered as (

    select *
    from date_spine
    where date_day <= current_date()

),

dim_date as (

    select

        to_char(date_day, 'YYYYMMDD')::int as date_key,
        date_day,

        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,

        weekofyear(date_day) as week_of_year,
        dayofmonth(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week_number,
        dayname(date_day) as day_name,

        case
            when dayofweek(date_day) in (0, 6) then true
            else false
        end as is_weekend,

        case
            when month(date_day) in (12, 1, 2) then 'Winter'
            when month(date_day) in (3, 4, 5) then 'Spring'
            when month(date_day) in (6, 7, 8) then 'Summer'
            else 'Autumn'
        end as season

    from filtered

)

select *
from dim_date