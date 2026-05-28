with source as (

    select * from {{ source('pagila', 'country') }}

),

renamed as (

    select
        country,
        country_id,
        last_update AS updated_at

    from source

)

select * from renamed
