with source as (

    select * from {{ source('pagila', 'film') }}

),

renamed as (

    select
        title,
        length,
        rating,
        film_id,
        fulltext,
        description,
        language_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        rental_rate,
        release_year,
        rental_duration,
        replacement_cost,
        special_features,
        original_language_id

    from source

)

select * from renamed
