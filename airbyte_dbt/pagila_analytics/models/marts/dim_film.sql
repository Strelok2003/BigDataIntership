with film as (

    select * from {{ ref('stg__film') }}

),

film_categories as (
    select
        film_id
        ,array_agg(category) as categories
    from {{ ref('int__film_category_bridge') }}
    group by film_id
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
        lang2.name as original_language,
        f_c.categories
    from film as film
    left join {{ ref('stg__language') }} as lang
        on film.language_id = lang.language_id
    left join {{ ref('stg__language') }} as lang2
        on film.original_language_id = lang2.language_id
    left join film_categories as f_c
        on film.film_id = f_c.film_id

)

select * from dim_film
