-- Organization dimension with enriched attributes
with organizations as (
    select * from {{ ref('stg_organizations') }}
),
org_metrics as (
    select * from {{ ref('int_organization_metrics') }}
)
select
    o.organization_id,
    o.organization_name,
    o.business_os_type,
    o.industry,
    o.employee_count,
    o.status,
    o.created_at,
    o.churned_at,
    datediff('day', o.created_at, current_date) as days_since_signup,
    case
        when o.employee_count <= 10 then 'Small'
        when o.employee_count <= 50 then 'Medium'
        when o.employee_count <= 200 then 'Large'
        else 'Enterprise'
    end as company_size,
    case
        when datediff('day', o.created_at, current_date) <= 30 then 'New'
        when datediff('day', o.created_at, current_date) <= 90 then 'Early'
        when datediff('day', o.created_at, current_date) <= 365 then 'Established'
        else 'Mature'
    end as account_age_segment,
    om.current_mrr,
    om.total_events,
    om.active_users,
    om.total_users,
    om.registered_users,
    round(100.0 * om.registered_users / nullif(om.total_users, 0), 2) as user_registration_rate
from organizations o
left join org_metrics om on o.organization_id = om.organization_id
