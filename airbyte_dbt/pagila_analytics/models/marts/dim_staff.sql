with staff as (

    select * from {{ ref('stg__staff') }}

),

dim_staff as (

    select
        email,
        is_active,
        staff_id,
        store_id,
        username,
        last_name,
        address_id,
        first_name,
        first_name || ' ' || last_name as full_name,
        updated_at

    from staff

)

select * from dim_staff
