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
        CONVERT_TIMEZONE('UTC', create_date) AS created_at,
        customer_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
