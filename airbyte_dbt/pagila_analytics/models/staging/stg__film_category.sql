with source as (

    select * from {{ source('pagila', 'film_category') }}

),

renamed as (

    select
        film_id,
        category_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at

    from source

)

select * from renamed
