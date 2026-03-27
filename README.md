# 🚀 SaaS Analytics Deep Dive: The Bloom Growth™ Story

> **From Raw Data to Actionable Insights** — A Data Engineering Detective Story

Welcome to an unconventional analytics project. This isn't just another dbt model repository. This is a **data investigation** that uncovers the hidden patterns in a SaaS platform serving 203 organizations. Ready to discover what the numbers reveal? 🔍

---

## 📖 The Mystery We're Solving

Imagine you're handed 17,030 events, 1,972 users, and 333 subscriptions across 4 different business operating systems. The question: **How does activity drive revenue? When do customers churn? Which segments actually matter?**

This project answers these burning questions through elegant data modeling and detective work:

- 🎯 **Activity vs. Revenue**: Do engaged customers actually spend more?
- ⚠️ **Churn Detection**: What are the early warning signs before customers leave?
- 💰 **Segment Analysis**: Which customer types generate the most value?
- 📊 **Platform Comparison**: How do different Business OS frameworks stack up?
- 📈 **Growth Patterns**: What engagement signals predict upgrades?

---

## 🏗️ The Architecture: Staging → Intermediate → Marts

This project follows **analytics engineering best practices** using the medallion architecture:

```
RAW DATA (CSV Seeds)
    ↓
STAGING LAYER (Data Cleaning & Validation)
    ├── stg_organizations      # Normalize and clean org records
    ├── stg_subscriptions      # Handle missing dates, infer status
    ├── stg_users             # Standardize user roles
    ├── stg_plans             # Price normalization
    ├── stg_activity_events   # Validate event categories
    ↓
INTERMEDIATE LAYER (Aggregation & Metrics)
    ├── int_organization_metrics    # MRR, user counts, activity
    ├── int_subscription_changes    # Track plan transitions
    ├── int_user_engagement        # Engagement scoring
    ├── int_activity_summary       # Feature usage breakdown
    ↓
MARTS LAYER (Business-Ready Analytics)
    ├── fct_customer_health       # Health scores & churn risk
    ├── fct_revenue_metrics       # MRR analysis
    ├── dim_organizations         # Organization dimension
    └── dim_subscriptions         # Subscription dimension
```

---

## 🛠️ Tech Stack: The Tools of the Trade

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **SQL Engine** | DuckDB 1.5.1 | Lightning-fast local analytics |
| **Transformation** | dbt-core 1.10.0 | Data modeling & orchestration |
| **Adapter** | dbt-duckdb | Connect dbt to DuckDB |
| **Scripting** | Python 3.14 | Automation & orchestration |
| **Data Handling** | pandas 3.0.1 | Analysis & reporting |

**Why DuckDB?** Fast, serverless, perfect for analytics without the infrastructure overhead.

---

## 🎬 Quick Start (5 Minutes)

### 1️⃣ Activate the Environment
```bash
source venv/bin/activate
```

### 2️⃣ Load & Transform Data
```bash
python load_seeds.py     # 📥 Load 17K events
python create_models.py  # 🔄 Transform data
```

### 3️⃣ Generate Insights
```bash
python generate_findings.py  # 📊 Answer all 5 questions
```

### 4️⃣ Explore the Database
```python
import duckdb
conn = duckdb.connect('analytics.duckdb')

# Query example:
results = conn.execute(
    "SELECT * FROM main.fct_customer_health LIMIT 10"
).fetch_all()
```

---

## 📁 File Explorer: What's Inside

```
📦 Project Root
├── 📄 README.md                          ← You are here!
├── 📄 FINDINGS.md                        ← 5 Key questions answered
├── 📄 AI_APPROACH.md                     ← How this was built
├── 📄 QUICKSTART.md                      ← Detailed setup guide
│
├── ⚙️ Configuration Files
│   ├── dbt_project.yml                   # dbt project config
│   ├── profiles.yml                      # DuckDB connection
│   └── .gitignore                        # Git exclusions
│
├── 🐍 Python Automation Scripts
│   ├── load_seeds.py                     # CSV → Database
│   ├── create_models.py                  # SQL execution engine
│   └── generate_findings.py              # Analysis reporter
│
├── 🌱 Raw Seeds (Data)
│   ├── organizations.csv                 # 203 orgs
│   ├── subscriptions.csv                 # 333 subscriptions
│   ├── users.csv                         # 1,972 users
│   ├── plans.csv                         # 4 pricing tiers
│   └── activity_events.csv               # 17,030 events
│
└── 📊 SQL Models (14 total)
    ├── staging/                          # 6 models - Data cleaning
    │   ├── stg_organizations.sql
    │   ├── stg_subscriptions.sql
    │   ├── stg_users.sql
    │   ├── stg_plans.sql
    │   ├── stg_activity_events.sql
    │   └── sources.yml
    │
    ├── intermediate/                     # 4 models - Aggregation
    │   ├── int_organization_metrics.sql
    │   ├── int_subscription_changes.sql
    │   ├── int_user_engagement.sql
    │   └── int_activity_summary.sql
    │
    └── marts/                            # 4 models - Analytics
        ├── fct_customer_health.sql
        ├── fct_revenue_metrics.sql
        ├── dim_organizations.sql
        └── dim_subscriptions.sql
```

---

## 🔑 Key Discoveries At A Glance

| Metric | Value | Insight |
|--------|-------|---------|
| **Total MRR** | $44,448 | Combined recurring revenue |
| **Retention Rate** | 77.3% | Healthy baseline |
| **Critical Risk** | 77 orgs | Need immediate attention |
| **Activity-Revenue Correlation** | 30.77% | Engagement matters but isn't automatic |
| **Best Platform** | EOS (5.3% churn) | Clear winner in retention |
| **Upgrade Rate** | 19.2% | 1 in 5 customers upgrade plans |

---

## 🚀 What Makes This Approach Interesting?

### 1. **Data Quality Craftsmanship** 🛠️
Instead of ignoring messy data, we _embrace_ it. Empty dates? Check. Invalid enums? Fixed. Missing values? Imputed intelligently. Every data quality issue becomes a learning opportunity.

### 2. **Storytelling with Numbers** 📖
This isn't just metrics. Each finding tells a story:
- **EOS platforms** outperform competitors by 8x in retention
- **Trial accounts** are 100% at-risk (critical insight!)
- **Activity ≠ Revenue** (30.77% conversion is the real stat)

### 3. **Architectural Thinking** 🏗️
Follow the medallion pattern:
- **Staging**: Clean and standardize
- **Intermediate**: Calculate metrics and aggregations
- **Marts**: Serve business questions directly

### 4. **Automation Over Manual Work** 🤖
Three Python scripts handle everything:
- Load 17K events in seconds
- Transform data in one command
- Generate insights automatically

---

## 📊 The Investigation Questions (All Answered!)

### ❓ Question 1: Activity-Revenue Correlation
**Finding**: 30.77% of high-activity orgs show MRR growth
- Engagement is necessary but not sufficient
- Other factors (capacity, demand) matter too
- Activity signals opportunity, not guarantee

### ❓ Question 2: Churn Risk Early Warnings
**Finding**: 77 organizations in critical risk (score 70+)
- Trial accounts = 100% churn risk
- Dormant users = strong churn indicator
- Declining MRR = clear signal

### ❓ Question 3: Most Valuable Segments
**Finding**: EOS generates $21,147 (47.6% of revenue)
- Platform choice drives value
- Medium companies ($22.9K total) punch above weight
- Large companies need nurturing ($368/mo avg)

### ❓ Question 4: Business OS Performance
**Finding**: 8x churn difference between platforms
- EOS: 5.3% churn (gold standard)
- Scaling Up: 42.1% churn (crisis)
- Platform choice is make-or-break

### ❓ Question 5: Engagement-Upgrade Patterns
**Finding**: 19.2% upgrade rate, +$377.69 avg expansion
- Engaged customers upgrade 34% more
- Feature usage predicts willingness to upgrade
- Expansion MRR = +$14,729 opportunity

---

## 🧬 Data DNA: What We're Analyzing

```
203 Organizations
├── 4 Different Business OS Frameworks
├── 3 Subscription Status Types (active, trial, churned)
└── 333 Active Subscriptions
    ├── 1,972 Users
    │   ├── 5 User Roles (admin, manager, etc)
    │   └── 3 Registration Statuses
    └── 17,030 Activity Events
        ├── 5 Feature Categories (meetings, scorecards, rocks, todos, issues)
        └── Usage Patterns Over Time
```

---

## 🎯 Next Steps

1. **Read FINDINGS.md** — Explore the 5 investigation questions with full data
2. **Run the project** — Follow the Quick Start above to see it in action
3. **Examine the SQL models** — Understand the data transformation logic
4. **Review AI_APPROACH.md** — See how this was built with AI assistance
5. **Explore the code** — Dive into individual SQL models to learn the technique

---

## 🔗 Quick Navigation

| Want to... | Go to... |
|----------|----------|
| See the findings | `FINDINGS.md` |
| Learn the technical approach | `AI_APPROACH.md` |
| Get step-by-step setup | `QUICKSTART.md` |
| Understand architecture | This file! 👈 |

---

## 💡 Why This Matters

This isn't an academic exercise. Real SaaS companies use these exact techniques to:
- ✅ Identify churn before it happens
- ✅ Understand which customers drive value
- ✅ Optimize product investments
- ✅ Guide sales strategies
- ✅ Inform retention programs

You're looking at production-grade analytics infrastructure. Enjoy! 🚀
