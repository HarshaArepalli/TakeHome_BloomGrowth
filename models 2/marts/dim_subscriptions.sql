-- Subscription dimension with plan details
with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),
plans as (
    select * from {{ ref('stg_plans') }}
)
select
    s.subscription_id,
    s.organization_id,
    s.plan_id,
    p.plan_name,
    s.seats,
    s.mrr_amount,
    p.base_price,
    p.per_seat_price,
    s.billing_interval,
    s.started_at,
    s.ended_at,
    s.change_reason,
    s.subscription_status,
    case
        when s.billing_interval = 'annual' then round(s.mrr_amount * 12, 2)
        else s.mrr_amount
    end as normalized_annual_value,
    case
        when s.subscription_status = 'active' then datediff('day', s.started_at, current_date)
        else datediff('day', s.started_at, s.ended_at)
    end as subscription_days
from subscriptions s
left join plans p on s.plan_id = p.plan_id
