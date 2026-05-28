with source as (

    select * from {{ source('pagila', 'category') }}

),

renamed as (

    select
        name,
        category_id,
        last_update AS updated_at

    from source

)

select * from renamed
