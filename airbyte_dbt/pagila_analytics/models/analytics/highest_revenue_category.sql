with rental_grouped_revenue as (
    select
        f_r.rental_id
        ,sum(f_r.amount) as total_revenue
    from {{ ref('fact_revenue') }} as f_r
    group by f_r.rental_id
)

select
    f_c.category
    ,sum(r_g_r.total_revenue) as total_revenue
from {{ ref('fact_rental') }} as f_r
join rental_grouped_revenue as r_g_r
    on r_g_r.rental_id = f_r.rental_id
join {{ ref('dim_inventory') }} as d_i
    on f_r.inventory_id = d_i.inventory_id
join {{ ref('int__film_category_bridge') }} as f_c
    on d_i.film_id = f_c.film_id
group by f_c.category
order by sum(r_g_r.total_revenue) desc
limit 1