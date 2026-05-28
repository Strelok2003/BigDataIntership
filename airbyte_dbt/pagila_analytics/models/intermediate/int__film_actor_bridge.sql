with film_actor as (
    select * from {{ ref('stg__film_actor') }}
),

film_actor_bridge as (
    select
        f_a.film_id
        ,film.title as film_title
        ,f_a.actor_id
        ,actor.first_name || ' ' || actor.last_name as actor_full_name
        ,f_a.updated_at
    from film_actor as f_a
    left join {{ ref('stg__film') }} as film
        on f_a.film_id = film.film_id
    left join {{ ref('stg__actor') }} as actor
        on f_a.actor_id = actor.actor_id
)

select *
from film_actor_bridge