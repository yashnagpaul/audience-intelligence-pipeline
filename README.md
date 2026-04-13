# Audience Intelligence Pipeline

Audience Intelligence Pipeline is a small Databricks-ready project that models a fan 360 dataset for sports and entertainment data.

## What this project does

The project unifies four source domains into one modeled audience table:

- ticket purchases
- website sessions
- email campaign engagement
- subscriptions and memberships

From those inputs, it builds fan-level outputs such as:

- total value
- engagement segment
- ticket purchase history
- website activity
- email open rate
- subscription status

## Files in this repo

- `databricks_pyspark_notebook.ipynb` - main Databricks / PySpark notebook
- `databricks_pyspark_notebook_README.md` - notebook-specific run instructions
- `audience_intelligence_local_demo.py` - local runnable companion using DuckDB and Pandas
- `audience_intelligence_requirements.txt` - local Python dependencies

## How to run locally

Install the local dependencies:

```bash
python3 -m pip install -r audience_intelligence_requirements.txt
```

Run the local demo:

```bash
python3 audience_intelligence_local_demo.py
```

The script writes these artifacts to `audience_intelligence_output/`:

- `fan_360.parquet`
- `segment_summary.csv`

## How to run in Databricks

1. Import `databricks_pyspark_notebook.ipynb` into a Databricks workspace.
2. Attach the notebook to a cluster with Delta Lake support.
3. Run the cells in order.
4. Review the modeled fan 360 output and the segment summary table.

Recommended cell flow:

- Cell 3: imports and Spark setup
- Cell 5: sample data creation
- Cell 7: fan 360 modeling
- Cell 8: Delta write and summary output
- Cell 10: incremental MERGE update
- Cell 12: data-quality checks
- Cell 14: performance notes
- Cell 16: widgets and job parameters