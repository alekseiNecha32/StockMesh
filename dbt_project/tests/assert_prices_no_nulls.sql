-- Fails if any price row has a null close after staging
select count(*) as null_close_count
from {{ ref('stg_prices') }}
where close is null
having count(*) > 0
