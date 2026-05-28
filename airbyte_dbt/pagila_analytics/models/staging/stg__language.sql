with source as (

    select * from {{ source('pagila', 'language') }}

),

renamed as (

    select
        name,
        language_id,
        last_update AS updated_at

    from source

)

select * from renamed
