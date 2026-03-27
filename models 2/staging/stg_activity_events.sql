with source_data as (
    select
        event_id,
        organization_id,
        user_id,
        feature_category,
        event_type,
        event_date,
        event_timestamp
    from {{ source('raw', 'activity_events') }}
)
select
    event_id,
    organization_id,
    user_id,
    case
        when feature_category in ('meetings', 'scorecards', 'rocks', 'todos', 'issues', 'headlines') 
            then feature_category
        else 'other'
    end as feature_category,
    event_type,
    event_date,
    event_timestamp
from source_data
order by event_timestamp
