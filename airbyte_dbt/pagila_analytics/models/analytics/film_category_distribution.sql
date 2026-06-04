select category,
        count(distinct film_id) as film_count
from {{ ref('int__film_category_bridge') }}
group by category