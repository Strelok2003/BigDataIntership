with source as (

    select * from {{ source('pagila', 'actor') }}

),

renamed as (

    select
        actor_id,
        last_name,
        first_name,
        last_update as updated_at

    from source

)

select * from renamed
