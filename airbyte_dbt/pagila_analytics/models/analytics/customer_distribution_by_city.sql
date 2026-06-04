select city,
        count(distinct customer_id) as customer_count
from {{ ref('dim_customer') }}
group by city