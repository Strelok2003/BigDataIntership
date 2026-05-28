with top_5_rented_films as (
    select 
        film.film_id,
        count(1) as rent_count
    from {{ ref('fact_rental') }} as rent
    join {{ ref('dim_inventory') }} as inve
        on rent.inventory_id = inve.inventory_id
    join {{ ref('dim_film') }} as film
        on inve.film_id = film.film_id
    group by film.film_id
    order by rent_count desc
    limit 5
),


actors_of_top_rented_films as (
    select 
        film_actor.film_title
        ,film_actor.actor_full_name
    from top_5_rented_films as top_film
    join {{ ref('int__film_actor_bridge') }} as film_actor
        on top_film.film_id = film_actor.film_id
)

select *
from actors_of_top_rented_films