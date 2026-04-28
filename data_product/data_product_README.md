# UK Housing & Credit Market — Quarterly Monitor

## What is this?

A quarterly dataset combining three UK public sources into a single analytical table:
- **HM Land Registry** — property transaction volumes and prices (1995–2026)
- **Bank of England** — monthly mortgage approval counts (1993–2026)
- **MLAR** — mortgage lending flows and balances (2007–2025)

132 quarters, one row per quarter, 18 business columns. Ready for analysis, visualization, or integration.

---

## Quick Start

### Download the SQLite file

1. Download `housing_credit_quarterly.db` from this folder
2. Open with any SQLite tool (DB Browser, DBeaver, Python, R)

**Python example:**
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("housing_credit_quarterly.db")
df = pd.read_sql("SELECT * FROM housing_credit_summary", conn)
conn.close()

print(df.shape)       # (132, 18)
print(df.tail())
```

### Or download the CSV file

Download `housing_credit_quarterly.csv` and open with Excel, Google Sheets, pandas, or any tool that reads CSV.

```python
import pandas as pd
df = pd.read_csv("housing_credit_quarterly.csv")
```

---


## Quick Visualization Example

Once you have downloaded the CSV, paste the code below to create your first visualization — no additional setup required.
```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("housing_credit_quarterly.csv")
df["price_avg__LR__gbp"] = pd.to_numeric(df["price_avg__LR__gbp"], errors="coerce")
df["quarter_start__date"] = pd.to_datetime(df["quarter_start__date"])
df = df.dropna(subset=["price_avg__LR__gbp"]).sort_values("quarter_start__date")

plt.figure(figsize=(12, 4))
plt.plot(df["quarter_start__date"], df["price_avg__LR__gbp"])
plt.title("Average House Price (GBP) — 1995 to 2026")
plt.ylabel("GBP")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()
```

You should see a chart of average UK house prices over 30+ years — useful as a quick sanity check that the data loaded correctly.

---
## Schema

| Column | Type | Description |
|--------|------|-------------|
| quarter_start_date | DATE | First day of the quarter (primary key) |
| quarter_label | TEXT | Human-readable label (e.g. "2024Q3") |
| transactions_total_lr_count | INTEGER | Total property transactions in the quarter |
| price_avg_lr_gbp | NUMERIC | Average transaction price (GBP) |
| price_median_lr_gbp | NUMERIC | Median transaction price (GBP) |
| price_min_lr_gbp | NUMERIC | Minimum transaction price (GBP) |
| price_max_lr_gbp | NUMERIC | Maximum transaction price (GBP) |
| boe_house_purchase_lpmvtvx_count | NUMERIC | BoE mortgage approvals for house purchase (quarterly avg) |
| boe_remortgage_lpmb4b3_count | NUMERIC | BoE mortgage approvals for remortgaging (quarterly avg) |
| boe_total_secured_lending_lpmb3c8_count | NUMERIC | BoE total secured lending approvals (quarterly avg) |
| boe_mfi_total_approvals_lpmz3up_count | NUMERIC | BoE MFI total approvals (quarterly avg) |
| mlar_gross_advances_mlar_1_21_c_1_gbp | NUMERIC | MLAR gross advances — regulated business flows (GBP) |
| mlar_net_advances_mlar_1_21_c_2_gbp | NUMERIC | MLAR net advances — regulated business flows (GBP) |
| mlar_new_commitments_mlar_1_21_c_3_gbp | NUMERIC | MLAR new commitments — regulated business flows (GBP) |
| mlar_imp_repayment_mlar_1_32_c_3_pct | NUMERIC | MLAR impaired credit — repayment advances (%) |
| mlar_imp_interest_only_mlar_1_32_c_4_pct | NUMERIC | MLAR impaired credit — interest-only advances (%) |
| mlar_new_house_purchase_mlar_1_33_c_29_gbp | NUMERIC | MLAR new house purchase advances (GBP) |
| mlar_new_remortgage_mlar_1_33_c_30_gbp | NUMERIC | MLAR new remortgage advances (GBP) |

---

## Quality Metrics

| Metric | Value | Note |
|--------|-------|------|
| Completeness (LR) | 100% from 1995Q1 | Land Registry records begin 1995 |
| Completeness (BoE) | 100% from 2000Q1 | Two series (remortgage, total lending) begin 1999; house purchase and MFI approvals complete from 1993 |
| Completeness (MLAR) | 97.4% from 2007Q1 | Latest 1–2 quarters may lag behind MLAR publication schedule |
| Freshness | Latest quarter: 2026Q1, last pipeline run: April 2026 | |
| Source coverage | 56.8% (75 of 132 quarters have all 3 sources) | Full three-source coverage from 2007Q1 |
| Uniqueness (PK) | 100% (132 unique quarters) | |
| Validity (price) | 100% for LR-available quarters | Price > 0, count > 0, date not in future |

---

## Data Contract

See [data_product_contract.yaml](data_product_contract.yaml) for the full machine-readable specification.

---

## Repository

Full pipeline source code: https://github.com/s24350/realestate-pipeline
