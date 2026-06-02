with source as (

    select * from {{ source('pagila', 'address') }}

),

renamed as (

    select
        phone,
        address,
        city_id,
        address2,
        district,
        address_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        postal_code:: integer AS postal_code

    from source

)

select * from renamed
