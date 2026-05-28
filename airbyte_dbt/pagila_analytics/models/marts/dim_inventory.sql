with inventory as (

    select * from {{ ref('stg__inventory') }}

),

dim_inventory as (

    select
        film_id,
        store_id,
        updated_at,
        inventory_id

    from inventory

)

select * from dim_inventory
