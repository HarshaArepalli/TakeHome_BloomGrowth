with source_data as (
    select
        plan_id,
        plan_name,
        base_price,
        per_seat_price
    from {{ source('raw', 'plans') }}
)
select
    plan_id,
    plan_name,
    coalesce(base_price, 0) as base_price,
    coalesce(per_seat_price, 0) as per_seat_price
from source_data
