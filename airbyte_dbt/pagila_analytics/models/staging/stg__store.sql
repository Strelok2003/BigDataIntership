with source as (

    select * from {{ source('pagila', 'store') }}

),

renamed as (

    select
        store_id,
        address_id,
        last_update AS updated_at,
        manager_staff_id

    from source

)

select * from renamed
