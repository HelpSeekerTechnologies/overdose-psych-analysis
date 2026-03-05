"""Run all 12 blog queries and save to blog_data.json."""
import sys, json
sys.path.insert(0, 'C:/Users/alina/overdose-psych-analysis')
from dbx_sql import run_sql

queries = {
    "Q1": {
        "label": "National opioid death trajectory 2016-2025",
        "sql": """SELECT timeframe, indicator_value, unit
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND measure_detail = 'Overall numbers' AND geography = 'Canada'
  AND time_granularity = 'By year' AND aggregator IS NULL
ORDER BY timeframe"""
    },
    "Q2": {
        "label": "Provincial opioid death rates per 100K by year",
        "sql": """SELECT geography, timeframe, indicator_value
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND measure_detail = 'Overall numbers' AND unit = 'Crude rate'
  AND time_granularity = 'By year' AND aggregator IS NULL
  AND geography IN ('British Columbia', 'Alberta', 'Ontario', 'Quebec', 'Saskatchewan', 'Manitoba')
ORDER BY geography, timeframe"""
    },
    "Q3": {
        "label": "Fentanyl vs non-fentanyl opioid deaths by year",
        "sql": """SELECT timeframe, aggregator, indicator_value
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND unit = 'Number' AND geography = 'Canada'
  AND time_granularity = 'By year'
  AND aggregator IN ('Fentanyl or fentanyl analogues', 'Non-fentanyl opioids')
ORDER BY timeframe, aggregator"""
    },
    "Q4": {
        "label": "Deaths by age group and sex (2023, Canada)",
        "sql": """SELECT aggregator AS age_group, disaggregator AS sex, indicator_value
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND measure_detail = 'Sex and age group' AND unit = 'Number'
  AND geography = 'Canada' AND time_granularity = 'By year'
  AND timeframe = '2023'
ORDER BY aggregator, disaggregator"""
    },
    "Q5": {
        "label": "Manner of death breakdown",
        "sql": """SELECT timeframe, aggregator, disaggregator, indicator_value
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND unit = 'Number' AND geography = 'Canada'
  AND time_granularity = 'By year'
  AND (measure_detail LIKE '%manner%' OR measure_detail LIKE '%Manner%' OR measure_detail LIKE '%accidental%' OR measure_detail LIKE '%intent%')
ORDER BY timeframe DESC
LIMIT 20"""
    },
    "Q6": {
        "label": "Stimulant deaths trajectory (Canada)",
        "sql": """SELECT timeframe, indicator_value, unit
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Stimulants' AND characteristic = 'Deaths'
  AND measure_detail = 'Overall numbers' AND geography = 'Canada'
  AND time_granularity = 'By year' AND aggregator IS NULL
ORDER BY timeframe"""
    },
    "Q7": {
        "label": "ED visits + hospitalizations + EMS (Canada, by year)",
        "sql": """SELECT characteristic, timeframe, indicator_value, unit
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND measure_detail = 'Overall numbers'
  AND geography = 'Canada' AND time_granularity = 'By year'
  AND aggregator IS NULL AND unit = 'Number'
  AND characteristic IN ('Deaths', 'Emergency Department (ED) Visits', 'Hospitalizations', 'Emergency Medical Services (EMS)')
ORDER BY characteristic, timeframe"""
    },
    "Q8": {
        "label": "Psychiatry payments vs overdose deaths",
        "sql": """SELECT geography, calendar_year, opioid_deaths_total, opioid_death_rate_per_100k,
       psychiatry_payments_thousands, psychiatry_pct_of_total_payments,
       all_physician_total_payments
FROM public_data.substance_use.overdose_vs_psychiatry_2021_2024
WHERE region_type IN ('Large Province', 'National')
ORDER BY geography, calendar_year"""
    },
    "Q9": {
        "label": "All specialty payments comparison (2023)",
        "sql": """SELECT specialty,
  SUM(total_payments_thousands) AS total_payments_thousands_2023
FROM public_data.substance_use.cihi_physician_payments_2021_2024
WHERE timeframe = 2023
GROUP BY specialty
ORDER BY total_payments_thousands_2023 DESC"""
    },
    "Q10": {
        "label": "Quebec vs BC vs Alberta deep comparison",
        "sql": """SELECT geography, timeframe, characteristic, unit, indicator_value
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND measure_detail = 'Overall numbers'
  AND geography IN ('British Columbia', 'Alberta', 'Quebec')
  AND time_granularity = 'By year' AND aggregator IS NULL
ORDER BY geography, characteristic, timeframe"""
    },
    "Q_hero": {
        "label": "Total opioid deaths 2016-2024 (Canada)",
        "sql": """SELECT SUM(indicator_value) AS total_deaths
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths'
  AND measure_detail = 'Overall numbers' AND unit = 'Number'
  AND geography = 'Canada' AND time_granularity = 'By year'
  AND aggregator IS NULL
  AND timeframe NOT LIKE '%2025%'"""
    },
    "Q_meta": {
        "label": "Distinct measure details for deaths",
        "sql": """SELECT DISTINCT measure_detail, aggregator, disaggregator
FROM public_data.substance_use.phac_substance_harms_2016_2025
WHERE substance_type = 'Opioids' AND characteristic = 'Deaths' AND unit = 'Number'
  AND geography = 'Canada' AND time_granularity = 'By year'
ORDER BY measure_detail, aggregator, disaggregator"""
    }
}

results = {}
for key, q in queries.items():
    print(f"\n--- {key}: {q['label']} ---")
    result = run_sql(q["sql"], key)
    if result:
        results[key] = result
    else:
        results[key] = {"columns": [], "data": [], "error": "Query returned no result"}

# Save
out_path = "C:/Users/alina/overdose-psych-analysis/data/blog_data.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\n{'='*50}")
print(f"Saved to {out_path}")
print(f"{'='*50}")
print(f"\nRow counts:")
for key in queries:
    r = results.get(key, {})
    rows = len(r.get("data", []))
    status = "OK" if rows > 0 else "EMPTY"
    print(f"  {key:8s}: {rows:4d} rows  [{status}]")
