with source as (

    select * from {{ source('pagila', 'inventory') }}

),

renamed as (

    select
        film_id,
        store_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        inventory_id,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
