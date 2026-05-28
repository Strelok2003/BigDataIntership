with film as (

    select * from {{ ref('stg__film') }}

),

dim_film as (

    select
        film.title,
        film.length,
        film.rating,
        film.film_id,
        film.description,
        lang.name as language,
        film.updated_at,
        film.rental_rate,
        film.release_year,
        film.rental_duration,
        film.replacement_cost,
        film.special_features,
        lang2.name as original_language

    from film as film
    left join {{ ref('stg__language') }} as lang
        on film.language_id = lang.language_id
    left join {{ ref('stg__language') }} as lang2
        on film.original_language_id = lang2.language_id

)

select * from dim_film
