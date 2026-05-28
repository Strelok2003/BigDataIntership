with source as (

    select * from {{ source('pagila', 'customer') }}

),

renamed as (

    select
        email,
        store_id,
        last_name,
        activebool AS is_active,
        address_id,
        first_name,
        create_date AS created_at,
        customer_id,
        last_update AS updated_at

    from source

)

select * from renamed
