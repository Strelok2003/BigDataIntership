with source as (

    select * from {{ source('pagila', 'inventory') }}

),

renamed as (

    select
        film_id,
        store_id,
        last_update AS updated_at,
        inventory_id

    from source

)

select * from renamed
