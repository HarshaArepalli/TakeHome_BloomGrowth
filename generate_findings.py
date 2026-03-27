#!/usr/bin/env python3
"""
Generate analysis findings from the dbt models.
"""
import duckdb
import json
from datetime import datetime

db_path = "/Users/shivachaithanyagoli/Desktop/harsha_interview_assessment/analytics.duckdb"
conn = duckdb.connect(db_path)

print("=" * 80)
print("ANALYTICS ENGINEER TAKE-HOME ASSESSMENT - ANALYSIS FINDINGS")
print("=" * 80)
print()

# 1. Activity levels correlation with MRR changes
print("1. ACTIVITY LEVELS CORRELATION WITH MRR CHANGES")
print("-" * 80)

activity_mrr = conn.execute("""
SELECT
    om.organization_id,
    om.organization_name,
    om.current_mrr,
    om.total_events,
    om.active_users,
    rm.expansion_mrr,
    rm.contraction_mrr,
    rm.net_mrr_change
FROM int_organization_metrics om
LEFT JOIN fct_revenue_metrics rm ON om.organization_id = rm.organization_id
WHERE om.total_events > 0
ORDER BY om.total_events DESC
LIMIT 10;
""").fetchdf()

print("\nTop 10 Active Organizations:")
print(activity_mrr[['organization_name', 'current_mrr', 'total_events', 'active_users', 'net_mrr_change']].to_string(index=False))

# Calculate correlation
active_orgs = conn.execute("""
SELECT
    COUNT(DISTINCT om.organization_id) as total_organizations,
    COUNT(DISTINCT CASE WHEN om.total_events > 100 THEN om.organization_id END) as high_activity_orgs,
    COUNT(DISTINCT CASE WHEN om.total_events > 100 AND rm.net_mrr_change > 0 THEN om.organization_id END) as high_activity_with_mrr_growth,
    round(100.0 * COUNT(DISTINCT CASE WHEN om.total_events > 100 AND rm.net_mrr_change > 0 THEN om.organization_id END) / 
        nullif(COUNT(DISTINCT CASE WHEN om.total_events > 100 THEN om.organization_id END), 0), 2) as pct_high_activity_with_growth
FROM int_organization_metrics om
LEFT JOIN fct_revenue_metrics rm ON om.organization_id = rm.organization_id;
""").fetchall()

print("\n\nActivity & MRR Correlation Summary:")
print(f"  - Total Organizations: {active_orgs[0][0]}")
print(f"  - High Activity Organizations (>100 events): {active_orgs[0][1]}")
print(f"  - High Activity with MRR Growth: {active_orgs[0][2]}")
print(f"  - % of High Activity with Growth: {active_orgs[0][3]}%")

# 2. Churn risk indicators
print("\n\n2. EARLY WARNING SIGNS FOR CHURN RISK")
print("-" * 80)

churn_risk = conn.execute("""
SELECT
    organization_name,
    status,
    current_mrr,
    churn_risk_score,
    health_score,
    CASE 
        WHEN churn_risk_score >= 70 THEN 'Critical'
        WHEN churn_risk_score >= 50 THEN 'High'
        WHEN churn_risk_score >= 30 THEN 'Medium'
        ELSE 'Low'
    END as risk_level,
    active_user_count,
    at_risk_user_count,
    dormant_user_count
FROM fct_customer_health
WHERE status != 'churned'
ORDER BY churn_risk_score DESC
LIMIT 15;
""").fetchdf()

print("\nTop 15 Organizations at Churn Risk:")
print(churn_risk[['organization_name', 'status', 'current_mrr', 'churn_risk_score', 'risk_level', 'at_risk_user_count', 'dormant_user_count']].to_string(index=False))

churn_stats = conn.execute("""
SELECT
    COUNT(*) as total_at_risk,
    COUNT(CASE WHEN churn_risk_score >= 70 THEN 1 END) as critical,
    COUNT(CASE WHEN churn_risk_score >= 50 AND churn_risk_score < 70 THEN 1 END) as high,
    COUNT(CASE WHEN churn_risk_score >= 30 AND churn_risk_score < 50 THEN 1 END) as medium,
    COUNT(CASE WHEN status = 'churned' THEN 1 END) as churned
FROM fct_customer_health;
""").fetchall()

print("\nChurn Risk Distribution:")
print(f"  - Critical Risk (70+): {churn_stats[0][1]} organizations")
print(f"  - High Risk (50-69): {churn_stats[0][2]} organizations")
print(f"  - Medium Risk (30-49): {churn_stats[0][3]} organizations")
print(f"  - Already Churned: {churn_stats[0][4]} organizations")

# 3. Most valuable customer segments
print("\n\n3. MOST VALUABLE CUSTOMER SEGMENTS")
print("-" * 80)

valuable_segments = conn.execute("""
SELECT
    business_os_type,
    COUNT(*) as org_count,
    ROUND(AVG(current_mrr), 2) as avg_mrr,
    ROUND(SUM(current_mrr), 2) as total_mrr,
    ROUND(AVG(total_events), 2) as avg_events,
    ROUND(AVG(active_users), 0) as avg_active_users,
    ROUND(100.0 * COUNT(CASE WHEN status = 'active' THEN 1 END) / COUNT(*), 1) as pct_active
FROM dim_organizations
GROUP BY business_os_type
ORDER BY total_mrr DESC;
""").fetchdf()

print("\nRevenue by Business OS Type:")
print(valuable_segments.to_string(index=False))

segment_size = conn.execute("""
SELECT
    company_size,
    COUNT(*) as org_count,
    ROUND(AVG(current_mrr), 2) as avg_mrr,
    ROUND(SUM(current_mrr), 2) as total_mrr,
    ROUND(100.0 * COUNT(CASE WHEN status = 'active' THEN 1 END) / COUNT(*), 1) as pct_active
FROM dim_organizations
WHERE company_size IS NOT NULL
GROUP BY company_size
ORDER BY total_mrr DESC;
""").fetchdf()

print("\n\nRevenue by Company Size:")
print(segment_size.to_string(index=False))

# 4. Business OS type performance
print("\n\n4. BUSINESS OS TYPE PERFORMANCE")
print("-" * 80)

os_performance = conn.execute("""
SELECT
    dim_orgs.business_os_type,
    COUNT(*) as total_orgs,
    COUNT(CASE WHEN dim_orgs.status = 'active' THEN 1 END) as active_orgs,
    COUNT(CASE WHEN dim_orgs.status = 'churned' THEN 1 END) as churned_orgs,
    ROUND(100.0 * COUNT(CASE WHEN dim_orgs.status = 'churned' THEN 1 END) / COUNT(*), 1) as churn_rate_pct,
    ROUND(AVG(dim_orgs.current_mrr), 2) as avg_mrr,
    ROUND(AVG(dim_orgs.total_events), 0) as avg_engagement,
    ROUND(AVG(health.health_score), 1) as avg_health_score,
    ROUND(AVG(health.churn_risk_score), 1) as avg_churn_risk
FROM dim_organizations dim_orgs
LEFT JOIN fct_customer_health health ON dim_orgs.organization_id = health.organization_id
GROUP BY dim_orgs.business_os_type
ORDER BY avg_mrr DESC;
""").fetchdf()

print("\nBusiness OS Type Performance Metrics:")
print(os_performance.to_string(index=False))

# 5. Engagement patterns and plan upgrades
print("\n\n5. ENGAGEMENT PATTERNS & PLAN UPGRADE ANALYSIS")
print("-" * 80)

upgrade_engagement = conn.execute("""
SELECT
    om.organization_id,
    om.organization_name,
    om.total_events,
    om.active_users,
    om.categories_used,
    rm.expansion_count,
    rm.net_mrr_change,
    CASE WHEN rm.expansion_count > 0 THEN 'Upgraded' ELSE 'No Upgrade' END as upgrade_status
FROM int_organization_metrics om
LEFT JOIN fct_revenue_metrics rm ON om.organization_id = rm.organization_id
WHERE rm.expansion_count > 0
ORDER BY rm.net_mrr_change DESC
LIMIT 10;
""").fetchdf()

print("\nTop Organizations with Plan Upgrades:")
print(upgrade_engagement[['organization_name', 'total_events', 'active_users', 'categories_used', 'expansion_count', 'net_mrr_change']].to_string(index=False))

upgrade_stats = conn.execute("""
SELECT
    COUNT(DISTINCT om.organization_id) as total_organizations,
    COUNT(DISTINCT CASE WHEN rm.expansion_count > 0 THEN om.organization_id END) as organizations_with_upgrades,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN rm.expansion_count > 0 THEN om.organization_id END) / 
        COUNT(DISTINCT om.organization_id), 1) as pct_upgraded,
    ROUND(AVG(CASE WHEN rm.expansion_count > 0 THEN om.total_events END), 0) as avg_events_upgraded_orgs,
    ROUND(AVG(CASE WHEN rm.expansion_count = 0 THEN om.total_events END), 0) as avg_events_non_upgraded_orgs
FROM int_organization_metrics om
LEFT JOIN fct_revenue_metrics rm ON om.organization_id = rm.organization_id;
""").fetchall()

print("\n\nUpgrade Engagement Analysis:")
print(f"  - Total Organizations: {upgrade_stats[0][0]}")
print(f"  - Organizations with Upgrades: {upgrade_stats[0][1]}")
print(f"  - Percentage with Upgrades: {upgrade_stats[0][2]}%")
print(f"  - Avg Events (Upgraded Orgs): {upgrade_stats[0][3]}")
print(f"  - Avg Events (Non-Upgraded Orgs): {upgrade_stats[0][4]}")

# Feature usage by engagement
feature_usage = conn.execute("""
SELECT
    feature_category,
    COUNT(DISTINCT organization_id) as orgs_using,
    SUM(user_count) as total_users,
    SUM(event_count) as total_events,
    ROUND(AVG(event_count), 0) as avg_events_per_org
FROM int_activity_summary
GROUP BY feature_category
ORDER BY total_events DESC;
""").fetchdf()

print("\n\nFeature Category Usage:")
print(feature_usage.to_string(index=False))

# 6. Key metrics summary
print("\n\n6. KEY METRICS SUMMARY")
print("-" * 80)

summary = conn.execute("""
SELECT
    COUNT(DISTINCT organization_id) as total_organizations,
    COUNT(DISTINCT CASE WHEN status = 'active' THEN organization_id END) as active_customers,
    COUNT(DISTINCT CASE WHEN status = 'churned' THEN organization_id END) as churned_customers,
    COUNT(DISTINCT CASE WHEN status = 'trial' THEN organization_id END) as trial_accounts,
    ROUND(SUM(current_mrr), 2) as total_mrr,
    ROUND(AVG(current_mrr), 2) as avg_mrr_per_customer,
    ROUND(SUM(total_events), 0) as total_platform_events,
    COUNT(DISTINCT CASE WHEN total_users > 0 THEN organization_id END) as organizations_with_users
FROM int_organization_metrics;
""").fetchall()

print(f"\nPlatform-wide Metrics:")
print(f"  - Total Organizations: {summary[0][0]}")
print(f"  - Active Customers: {summary[0][1]}")
print(f"  - Churned Customers: {summary[0][2]}")
print(f"  - Trial Accounts: {summary[0][3]}")
print(f"  - Total MRR: ${summary[0][4]:,.2f}")
print(f"  - Avg MRR per Customer: ${summary[0][5]:,.2f}")
print(f"  - Total Platform Events: {summary[0][6]:,.0f}")
print(f"  - Organizations with Users: {summary[0][7]}")

churn_rate = conn.execute("""
SELECT
    ROUND(100.0 * COUNT(CASE WHEN status = 'churned' THEN 1 END) / COUNT(*), 1) as churn_rate_pct,
    ROUND(100.0 * COUNT(CASE WHEN status = 'active' THEN 1 END) / COUNT(*), 1) as retention_rate_pct
FROM int_organization_metrics;
""").fetchall()

print(f"  - Churn Rate: {churn_rate[0][0]}%")
print(f"  - Retention Rate: {churn_rate[0][1]}%")

print("\n" + "=" * 80)
print("END OF FINDINGS")
print("=" * 80)

conn.close()
