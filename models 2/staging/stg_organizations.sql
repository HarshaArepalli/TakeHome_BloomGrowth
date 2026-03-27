with source_data as (
    select
        organization_id,
        organization_name,
        business_os_type,
        industry,
        employee_count,
        status,
        created_at,
        churned_at
    from {{ source('raw', 'organizations') }}
)
select
    organization_id,
    organization_name,
    case
        when business_os_type in ('EOS', 'Pinnacle', 'Scaling Up', 'Custom') then business_os_type
        else 'Custom'
    end as business_os_type,
    industry,
    coalesce(employee_count, 0) as employee_count,
    case
        when status in ('active', 'churned', 'trial') then status
        else 'trial'
    end as status,
    created_at,
    case
        when status = 'churned' and churned_at is null then created_at
        else churned_at
    end as churned_at
from source_data
