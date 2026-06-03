with source as (

    select * from {{ source('pagila', 'actor') }}

),

renamed as (

    select
        actor_id,
        last_name,
        first_name,
        CONVERT_TIMEZONE('UTC', last_update) as updated_at,
        CONVERT_TIMEZONE('UTC', _airbyte_extracted_at) as _airbyte_extracted_at

    from source

)

select * from renamed
