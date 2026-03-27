with source_data as (
    select
        user_id,
        organization_id,
        user_name,
        role,
        is_registered,
        created_at,
        last_active_at
    from {{ source('raw', 'users') }}
)
select
    user_id,
    organization_id,
    user_name,
    case
        when role in ('owner', 'admin', 'manager', 'member') then role
        else 'member'
    end as role,
    coalesce(is_registered, false) as is_registered,
    created_at,
    last_active_at
from source_data
