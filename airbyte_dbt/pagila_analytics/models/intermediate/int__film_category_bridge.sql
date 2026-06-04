with film_category as (
    select * from {{ ref('stg__film_category') }}
),

film_category_bridge as (
    select
        f_c.film_id
        ,film.title as film_title
        ,cate.name as category
    from film_category as f_c
    left join {{ ref('stg__category') }} as cate
        on f_c.category_id = cate.category_id
    left join {{ ref('stg__film') }} as film
        on f_c.film_id = film.film_id
)

select *
from film_category_bridge