with rental_facts as (
    select * from {{ ref('int__rental_facts') }}
),

fact_rental as (
    select 
         rent.staff_id
        ,rent.rental_id
        ,rent.customer_id
        ,rent.updated_at

        ,rent.rental_datetime
        ,rent.return_datetime
        
        ,rent.rental_date
        ,rent.return_date
        
        ,to_char(rent.rental_date, 'YYYYMMDD'):: int as rental_date_key
        ,to_char(rent.return_date, 'YYYYMMDD'):: int as return_date_key

        ,rent.inventory_id
        ,datediff(day,rental_datetime, return_datetime) AS rental_duration_days
        ,datediff(hour,rental_datetime, return_datetime) AS rental_duration_hours
        ,case when return_date is null then true else false end as is_open_rental
    from rental_facts as rent
)

select *
from fact_rental