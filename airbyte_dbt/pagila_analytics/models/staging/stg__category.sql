with source as (

    select * from {{ source('pagila', 'category') }}

),

renamed as (

    select
        name,
        category_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
