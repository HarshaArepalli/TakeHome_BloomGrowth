with source_data as (
    select
        subscription_id,
        organization_id,
        plan_id,
        seats,
        mrr_amount,
        billing_interval,
        started_at,
        ended_at,
        change_reason
    from {{ source('raw', 'subscriptions') }}
)
select
    subscription_id,
    organization_id,
    plan_id,
    coalesce(seats, 1) as seats,
    coalesce(mrr_amount, 0) as mrr_amount,
    case
        when billing_interval in ('monthly', 'annual') then billing_interval
        else 'monthly'
    end as billing_interval,
    started_at,
    case
        when ended_at = '' or ended_at is null then null
        else ended_at
    end as ended_at,
    case
        when change_reason in ('new', 'upgrade', 'downgrade', 'churn', 'reactivation') then change_reason
        else 'new'
    end as change_reason,
    case when ended_at is not null then 'churned' else 'active' end as subscription_status
from source_data
order by organization_id, started_at
