with source as (

    select * from {{ source('pagila', 'language') }}

),

renamed as (

    select
        name,
        language_id,
        CONVERT_TIMEZONE('UTC', last_update) AS updated_at,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
