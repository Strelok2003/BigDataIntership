select film.*
from {{ ref('dim_film') }} as film
left join {{ ref('dim_inventory') }} as inve
    on film.film_id = inve.film_id
where inve.film_id is null
