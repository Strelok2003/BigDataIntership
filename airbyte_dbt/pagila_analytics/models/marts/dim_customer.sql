with customer_enriched as (
    select * from {{ ref('int__customer_enriched') }}
),

dim_customer as (
    select
        email
        ,store_id
        ,last_name
        ,is_active
        ,address_id
        ,first_name
        ,created_at
        ,to_char(created_at, 'YYYYMMDD'):: int as created_date_key
        ,customer_id
        ,updated_at
        ,address
        ,postal_code
        ,city
        ,country
    from customer_enriched
)

select * from dim_customer