**Big4-Style Retail Performance Deep Dive**

📌 Project Overview

This project simulates a Big4-style analytical case study:

A client — a large national retail chain with 500+ stores — is concerned that some outlets are underperforming and dragging down regional profitability.

The goal: identify drivers, explain gaps, and recommend actions.

Dataset: synthetic but realistic → 12 regions, 520 stores, ~7 months of daily sales with seasonality, income/population context, lifecycle, and anomalies.

🗂️ Data

regions.csv → macro factors (region, population, avg_income)

stores.csv → store master data (store_id, city, region, opening_date)

sales.csv → daily store transactions (store_id, date, revenue, transactions)

⚙️ Methodology

Data Cleaning (01_cleaning.sql)

Remove anomalies (tx=0 & revenue>0, negative revenue with tx>0).

Winsorize revenue (p01–p99) by region/day.

Deduplicate store-date.

Derive: AOV, store age buckets, per-capita revenue.

KPI Core (02_kpi_core.sql)

Executive KPIs (Revenue, Transactions, AOV).

Top/Bottom regions & stores.

Stores vs. regional peers (z-scores).

Weekday seasonality & trends.

Store League (quartiles).

Lifecycle cohorts.

Income ↔ AOV regression.

Regional Drivers (03_regional_factors.sql)

Per-capita normalization.

Income quintiles vs. AOV.

Decomposition: traffic vs. AOV contribution.

Population bins.

Store Lifecycle (04_store_lifecycle.sql)

Ramp-up curves (weeks since opening).

Cohort-by-month analysis.

Time-to-benchmark: weeks until a store meets network medians.

Store League & Risk (05_league_and_risk.sql)

Quartiles/deciles inside region.

z-scores for KPI vs peers.

Risk scoring (weighted).

Region-level watchlist (% of weak stores).

EDA Notebook (notebooks/eda.ipynb)

AOV distributions by region.

Income ↔ AOV scatter with regression.

Weekday seasonality.

Ramp-up visualization.

Risk heatmap (% stores below average).

🔍 Key Findings

Income elasticity: higher income → higher AOV (corr > 0).

Lifecycle effect: some cohorts ramp faster; regional differences matter.

Within-region dispersion: management/assortment plays a role beyond city economics.

Seasonality: weekends, summer, December spikes.

Risk pockets: some regions have >40% of stores under peer average.

💡 Recommendations

In low-income regions → focus on traffic (frequency, promotions).

For lagging stores in strong regions → management/assortment audit.

New stores → structured 30/60/90-day benchmarks, monitor TTB.

Seasonal optimization → align promotions with weekends/summer/Dec.

Use league dashboards to flag outliers continuously.

📊 Deliverables

SQL scripts (/sql/01_cleaning.sql … 05_league_and_risk.sql)

EDA notebook (/notebooks/eda.ipynb)

BI dashboard (Tableau/Power BI):

Executive Overview

Regional Drivers

Store League

Lifecycle Analysis

Risk & Anomalies

🚀 How to Reproduce

1. Load Data
2. 
# Postgres example

\copy raw.regions  FROM 'data/regions.csv'  CSV HEADER
\copy raw.stores   FROM 'data/stores.csv'   CSV HEADER
\copy raw.sales    FROM 'data/sales.csv'    CSV HEADER

2. Run SQL in order

01_cleaning.sql

02_kpi_core.sql

03_regional_factors.sql

04_store_lifecycle.sql

05_league_and_risk.sql

3. Run Python Notebook
jupyter notebook notebooks/eda.ipynb

4. Build BI Dashboard

Import sales_cleaned or v_sales_last90 into Tableau / Power BI.

Replicate 5 views (Overview, Regional, League, Lifecycle, Risk).

🖼️ Screenshots (placeholders)

Executive Dashboard

Regional Drivers

Store League

Lifecycle Ramp-up

Risk Heatmap

✨ This project demonstrates not just SQL and BI, but also business thinking & consulting-style insights — portfolio-ready for Big4/Middle Analyst case studies.
