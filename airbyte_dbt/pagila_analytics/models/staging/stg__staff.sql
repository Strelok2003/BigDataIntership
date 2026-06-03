with source as (

    select * from {{ source('pagila', 'staff') }}

),

renamed as (

    select
        email,
        active AS is_active,
        picture,
        password,
        staff_id,
        store_id,
        username,
        last_name,
        address_id,
        first_name,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
