-- Revenue analysis and MRR trends
with org_metrics as (
    select * from {{ ref('int_organization_metrics') }}
),
subscription_changes as (
    select * from {{ ref('int_subscription_changes') }}
),
subscription_summary as (
    select
        organization_id,
        sum(case when change_type = 'expansion' then mrr_change else 0 end) as expansion_mrr,
        sum(case when change_type = 'contraction' then abs(mrr_change) else 0 end) as contraction_mrr,
        count(distinct case when change_type = 'expansion' then subscription_id end) as expansion_count,
        count(distinct case when change_type = 'contraction' then subscription_id end) as contraction_count,
        count(distinct case when change_type = 'churn' then subscription_id end) as churn_count
    from subscription_changes
    where mrr_change is not null
    group by organization_id
)
select
    om.organization_id,
    om.organization_name,
    om.business_os_type,
    om.industry,
    om.status,
    om.current_mrr,
    om.max_mrr_ever,
    coalesce(ss.expansion_mrr, 0) as expansion_mrr,
    coalesce(ss.contraction_mrr, 0) as contraction_mrr,
    coalesce(ss.expansion_mrr, 0) - coalesce(ss.contraction_mrr, 0) as net_mrr_change,
    coalesce(ss.expansion_count, 0) as expansion_count,
    coalesce(ss.contraction_count, 0) as contraction_count,
    coalesce(ss.churn_count, 0) as churn_count,
    om.upgrade_count,
    om.downgrade_count,
    case
        when om.current_mrr = 0 then 0
        when om.upgrade_count > 0 then round((coalesce(ss.expansion_mrr, 0) / om.current_mrr) * 100, 2)
        else 0
    end as expansion_pct_of_mrr,
    case
        when om.max_mrr_ever = 0 then 0
        else round((om.current_mrr / om.max_mrr_ever) * 100, 2)
    end as mrr_retention_pct
from org_metrics om
left join subscription_summary ss on om.organization_id = ss.organization_id
order by om.current_mrr desc
