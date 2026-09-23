select
    name  as city_name,
    country,
    latitude,
    longitude
from {{ ref('cities') }}