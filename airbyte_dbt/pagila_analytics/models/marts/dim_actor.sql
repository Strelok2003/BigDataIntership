with actor as (

    select * from {{ ref('stg__actor') }}

),

dim_actor as (

    select
        actor_id,
        last_name,
        first_name,
        last_name || ' ' || first_name as full_name,
        updated_at

    from actor

)

select * from dim_actor
