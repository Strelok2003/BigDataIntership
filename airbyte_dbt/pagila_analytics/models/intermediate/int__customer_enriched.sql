with customers as (
    select * from {{ ref('stg__customer') }}
),

customer_enriched as (
    select 
        cust.email
        ,cust.store_id
        ,cust.last_name
        ,cust.is_active
        ,cust.address_id
        ,cust.first_name
        ,cust.created_at
        ,cust.customer_id
        ,cust.updated_at
        ,addr.address
        ,addr.postal_code
        ,city.city
        ,coun.country
    from customers as cust
    left join {{ ref('stg__address') }} as addr
        on cust.address_id = addr.address_id
    left join {{ ref('stg__city') }} as city
        on addr.city_id = city.city_id
    left join {{ ref('stg__country') }} as coun
        on city.country_id = coun.country_id
)


select *
from customer_enriched