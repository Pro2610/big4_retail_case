Big4-Style Retail Performance Deep Dive
📌 Project Overview

This project simulates a Big4-style analytical case:
A client — a large national retail chain with 500+ stores — is concerned that some outlets are underperforming and dragging down regional profitability.
As analysts, our task is to identify patterns, explain performance gaps, and provide actionable recommendations.

The dataset is synthetic but realistic: it includes 12 regions, 520 stores, ~7 months of daily sales with seasonal effects, income/population differences, store lifecycle, and embedded anomalies.

🗂️ Data Description

1. sales – daily store-level performance

store_id – store identifier

date – transaction date

revenue – daily revenue (EUR)

transactions – number of receipts

2. stores – store master data

store_id

city

region

opening_date

3. regions – regional macro factors

region

population

avg_income (monthly, EUR)

⚙️ Methodology
Data Cleaning

Filtered to last 90 days (quarter) for KPIs.

Removed anomalies:

transactions = 0 with positive revenue

Extreme negative revenues (returns > sales)

Winsorized outliers (1–99 percentile by region/day)

KPI Tree

Revenue = AOV × Transactions

AOV (Average Order Value) = revenue ÷ transactions

Transactions = store traffic

Regional context: per-capita revenue, income elasticity, store age effect.

Analytical Cuts (SQL)

Top regions by AOV

Underperforming stores vs. regional average

Store lifecycle segmentation (new vs. mature)

Income vs AOV correlation

Dispersion within regions (management effect)

“Store league” (quartiles inside region)

🔍 Key Findings

Regional demand drivers: Higher income → higher AOV (hypothesis confirmed).

Store lifecycle: Newly opened stores ramp up differently across regions; some lag behind benchmarks.

Within-region variance: Even in the same city/region, some stores are significantly below average → management/assortment issues.

Seasonality: Weekends, summer months, and December show clear peaks in revenue.

Data quality: Rare anomalies (negative days, test transactions) highlight the importance of cleaning.

💡 Recommendations

Low-income regions → focus on traffic growth (promotions, low-price bundles) instead of AOV.

Weak stores within strong regions → perform management audits (staff, assortment, competitor mapping).

New stores → implement a 30/60/90-day ramp-up playbook with benchmark tracking.

Seasonal optimization → align promotions with peak months/weekends for maximum ROI.

Regional dashboards → monitor store quartiles vs. peers and act on outliers.

📊 Deliverables

SQL scripts (/sql/) for cleaning & KPIs

EDA notebook (/notebooks/eda.ipynb) with visual checks (AOV distribution, correlation with income, seasonality)

Dashboard (Tableau/Power BI) with 5 views:

Executive Overview

Regional Drivers

Store League

Lifecycle Analysis

Anomalies & Data Quality
