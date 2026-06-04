with address as (
    select * from {{ ref('stg__address') }}
),

dim_address as (
    select 
        addr.phone
        ,addr.address
        ,city.city
        ,addr.address2
        ,addr.district
        ,addr.address_id
        ,addr.updated_at
        ,addr.postal_code
        ,coun.country
    from address as addr
    left join {{ ref('stg__city') }} as city
        on addr.city_id = city.city_id
    left join {{ ref('stg__country') }} as coun
        on city.country_id = coun.country_id
)

select *
from dim_address