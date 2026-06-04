with category_rent_hours as (
    select f_c.category,
            min(f_r.rental_duration_hours) as min_rent_hour,
            max(f_r.rental_duration_hours) as max_rent_hour,
            avg(f_r.rental_duration_hours) as avg_rent_hour
    from {{ ref('fact_rental') }} as f_r
    join {{ ref('dim_inventory') }} as d_i
        on f_r.inventory_id = d_i.inventory_id
    join {{ ref('int__film_category_bridge') }} as f_c
        on d_i.film_id = f_c.film_id
    where f_r.is_open_rental = false
    group by f_c.category
)

select *
from category_rent_hours