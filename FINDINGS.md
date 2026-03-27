# Analytics Assessment - What I Found in the Data

## Quick Overview

I spent time digging through 203 organizations on this platform and found some interesting patterns. The platform generates about $44K in monthly recurring revenue with a 77% retention rate, which is solid. But there are some red flags too—particularly around trial accounts and one struggling framework. Here's what stood out to me.

---

## 1. Does Activity Actually Drive Revenue?

### The Short Answer: Sort of, but it's complicated

I noticed that just having engaged users doesn't automatically mean revenue goes up. Out of 52 organizations with really high activity (over 100 events), only 16 actually saw MRR growth. That's about 31%. So engagement matters, but it's not the whole story.

Looking at the winners: **Pacific Services** was hitting the platform hard (596 events) and their MRR jumped by $445. But then you have **Broadleaf Partners** with 503 events that stayed flat at $199 MRR. Same activity level, different outcomes.

The organizations that did upgrade their plans? They averaged 106 events versus 79 for everyone else. That's a 34% difference. So there's definitely a correlation, just not a perfect one.

My take: Activity is a necessary ingredient but not sufficient by itself. You need other things too—maybe the customer hits a capacity wall, or they discover they actually need more features. The platform might be missing opportunities to connect activity spikes with upgrade prompts.

---

## 2. Early Warning Signs Before Customers Leave

### The Real Story: Trial Accounts Are a Crisis

Here's what I found:

- **77 organizations** are in critical risk territory (risk score 70+)
- **124 more** are in the high-risk zone (50-69)
- **33 customers** have already churned
- Overall churn rate is about **16.3%**

But the most concerning thing? All 13 trial accounts are at critical risk. Every single one. That's a 100% churn risk rate for trials. These accounts have $0 MRR by design, but most of them also have dormant users (averaging 4-5 inactive users per trial). This feels like a leaky bucket—trials aren't converting.

Beyond trials, I'm seeing a few other danger signals:

**Declining Revenue is a Big Red Flag.** When organizations drop below 50% of their historical peak MRR, their risk scores jump to 80+. **Northstar Corp** is a good example—they're active, but their MRR is only $64 (way down from where they were), they have a history of downgrades, and 2 dormant users. That's someone barely hanging on.

**Dormant Users Matter Too.** Organizations where more than half the users haven't done anything in 30+ days tend to score 60+ on risk. The pattern is consistent: if users go quiet for more than 30 days, risk goes up. More than 14 days? Still a concern.

I'd flag these as priority outreach targets: Northstar Corp ($64 MRR, risk 80), Pioneer Solutions ($64 MRR, risk 80), and Crestline Tech ($79 MRR, risk 70).

---

## 3. Where Is the Money Actually Coming From?

### Different Frameworks, Very Different Results

I was surprised by how much the business OS framework matters. EOS is clearly the star:

- **EOS**: 76 customers, averaging $278/month each, totaling $21K (basically half the platform's revenue)
- **Custom implementations**: 45 customers, $264 average, $11.9K total
- **Pinnacle**: 44 customers, $167 average, $7.3K total  
- **Scaling Up**: 38 customers, only $108 average, $4.1K total

What's wild is that EOS isn't even the largest segment by count (37% of customers), but it drives 48% of revenue. EOS customers are worth 2.6x more than Scaling Up customers on average.

Looking at company size, the picture is different:

- **Medium companies (11-50 people)**: Largest revenue pool ($22.9K total), even though they average $218/month each
- **Large companies (51-200 people)**: Smaller pool in total ($9.6K), but highest per-customer value at $369/month
- **Small companies (≤10 people)**: $12K total, averaging $167/month

So medium-sized businesses are your volume play, but big companies are your highest-value customers. Makes sense.

Here's what concerns me: **Scaling Up is broken**. It has a 42% churn rate, the lowest engagement (64 average events), and only 50% of customers are actually active. That's not a minor issue—that's a potential market fit problem.

---

## 4. Platform Performance: Why EOS Wins

I pulled together the performance data across frameworks and the differences are stark:

**EOS** is the clear winner:
- Only 5.3% churn (4 customers out of 76)
- Highest engagement (100 events average)
- Healthy customer base overall
- This is what good product-market fit looks like

**Custom and Pinnacle** are in the middle:
- Moderate churn (11-18%)
- Decent engagement (77-80 events)
- Could be better, could be worse

**Scaling Up** is struggling:
- 42% churn rate (16 of 38 customers left)
- Lowest engagement (64 events)
- Something is fundamentally wrong here

I'd want to understand why. Is it a product issue? Are the wrong customers getting put on Scaling Up? Is the onboarding process failing? Is it a pricing mismatch? Those 16 churned customers probably have answers.

---

## 5. Engagement and Upgrades: What Actually Drives Growth

### The Upgrade Story

Out of 203 organizations, 39 have upgraded to a higher plan. That's 19.2%—not bad, but there's room. The organizations that upgraded averaged 106 events, while non-upgraded averaged 79. So engaged customers are more likely to upgrade, which makes sense.

When they do upgrade, it's meaningful. The average expansion revenue is $378 per upgrade, and one customer (Pulse Works) added $490. Total expansion revenue across all upgrades: $14.7K.

**Top upgraders:**
- Pulse Works: 230 events, jumped +$490
- Pacific Services: 596 events, +$445  
- Oakwood Enterprises: 54 events, +$425

Interesting observation: The organizations that use all 6 feature categories (Rocks, Scorecards, Todos, Meetings, Issues, Headlines) seem to be the ones upgrading. Feature diversification correlates with expansion. Rocks and Scorecards seem to be the heavy hitters for engagement, consistently showing up in high-activity accounts.

Feature adoption itself is pretty even across the board—each feature is used by about 112-128 organizations. So features aren't siloed; customers are exploring the whole platform, which is good.

---

## What This All Means: My Recommendations

### Right Now (Next 30 Days)

1. **Fix the Trial Problem.** 13 trials at 100% risk with $0 MRR is your biggest leak. Offer extensions, lower entry pricing, or hand-hold through onboarding. Do something.

2. **Understand Scaling Up's Problem.** With 42% churn, something is broken. Talk to the customers who left. Are they going to competitors? Did the product not fit their workflow? Is pricing wrong?

3. **Save At-Risk Active Customers.** 15 active organizations with risk scores of 80+. These are salvageable. Personal outreach, demos of features they're not using, anything to show value.

### Next 30-90 Days

4. **Build an Upgrade Funnel.** Organizations at 70-100 events are ready to upgrade. Catch them at that point with targeted offers rather than hoping they discover upgrades on their own.

5. **Document EOS's Success.** Whatever EOS is doing right—better onboarding, clearer documentation, founder-market fit, whatever—write it down and apply it to the other frameworks.

6. **Monetize Custom Better.** Custom implementations have solid engagement (80 events average) but lower revenue than EOS. Are you leaving money on the table? Premium bundles? Professional services? Tiered pricing?

### The Longer Play (3+ Months Out)

7. **Build a Churn Predictor.** You've got enough signals (engagement drop, user dormancy, MRR trends) to build something that alerts you to churn risk early.

8. **Set Engagement Targets.** Scaling Up customers should be hitting 100 events within 90 days. Medium companies should hit X events. Create benchmarks and watch for gaps.

9. **Align Product Roadmap.** Rocks and Scorecards are the engagement drivers. Keep investing there.

---

## Technical Notes (For the Curious)

I built this using dbt with DuckDB and worked through the messier parts of the data—empty dates, missing values, inconsistent enums. The models are organized staging → intermediate → marts, so the data gets progressively cleaner and more useful as it moves through the pipeline.

- **Data as of**: March 27, 2026
- **Organizations analyzed**: 203
- **Events tracked**: 17,030
- **Users**: 1,972
- **Subscriptions**: 333

---

## The Bottom Line

This platform has real strengths: EOS is working, medium-sized companies love it, and when customers upgrade, it's meaningful ($378 average expansion). 

But there are real problems too: trial-to-paid conversion is completely broken (0%), Scaling Up framework is in crisis (42% churn), and there are about 77 organizations quietly at high risk of leaving.

The good news? With 203 customers and $44K MRR, you've got a solid foundation. The moves I outlined above could probably add $5-10K in annual revenue just by fixing the obvious leaks. Worth doing.
