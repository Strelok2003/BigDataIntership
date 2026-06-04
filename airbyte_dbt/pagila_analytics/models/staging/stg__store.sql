with source as (

    select * from {{ source('pagila', 'store') }}

),

renamed as (

    select
        store_id,
        address_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        manager_staff_id,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
