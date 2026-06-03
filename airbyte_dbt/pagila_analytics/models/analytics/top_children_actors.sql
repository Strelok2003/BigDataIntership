with children_films as (
    select f_c.film_id
    from {{ ref('int__film_category_bridge') }} as f_c
    where f_c.category = 'Children'
),

top_rated_children_films as (
    select d_f.film_id
    from {{ ref('dim_film') }} as d_f
    join children_films as c_f
        on d_f.film_id = c_f.film_id
    where d_f.rating = '' --need to clarify with busineess which one is highest rating
)

select f_a.actor_full_name,
        f_a.actor_id
from top_rated_children_films as t_r
join {{ ref('int__film_actor_bridge') }} as f_a
    on t_r.film_id = f_a.film_id