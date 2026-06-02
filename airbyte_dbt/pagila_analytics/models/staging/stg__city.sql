with source as (

    select * from {{ source('pagila', 'city') }}

),

renamed as (

    select
        city,
        city_id,
        country_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at

    from source

)

select * from renamed
