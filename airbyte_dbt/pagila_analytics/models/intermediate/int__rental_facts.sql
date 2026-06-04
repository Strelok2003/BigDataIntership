with rental as (
    select * from {{ ref('stg__rental') }}
),

rental_facts as (
    select 
        rent.staff_id
        ,rent.rental_id
        ,rent.customer_id
        ,rent.updated_at

        ,rent.rental_datetime
        ,rent.return_datetime
        
        ,rent.return_date
        ,rent.rental_date
        
        ,rent.inventory_id
        ,datediff(day,rental_datetime, return_datetime) AS rental_duration_days
        ,datediff(hour,rental_datetime, return_datetime) AS rental_duration_hours
        ,case when return_date is null then true else false end as is_open_rental
    from rental as rent

)

select *
from rental_facts