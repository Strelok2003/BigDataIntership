with source as (

    select * from {{ source('pagila', 'rental') }}

),

renamed as (

    select
        staff_id,
        rental_id,
        customer_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,

        CONVERT_TIMEZONE('UTC', rental_date) AS rental_datetime,
        rental_date::date AS rental_date,

        CONVERT_TIMEZONE('UTC', return_date) AS return_datetime,
        return_date::date AS return_date,
        inventory_id,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
