Audience Intelligence Pipeline — Databricks-ready demo

Overview

This mini-portfolio package is designed to be relevant to the Two Circles role.

It demonstrates how to build an audience intelligence pipeline that unifies ticketing, website, email, and subscription data into fan-level segmentation outputs for product and marketing teams.

This repository includes:
- `databricks_pyspark_notebook.ipynb`: a Databricks/PySpark notebook that models a fan 360 dataset, writes Delta outputs, and applies CDC-style updates.
- `audience_intelligence_local_demo.py`: a local DuckDB companion script that validates the same core modeling idea without requiring Databricks or a Java/Spark setup.
- `audience_intelligence_requirements.txt`: local dependencies for the DuckDB companion script.

Why this project fits the role

- mirrors lakehouse-style modeling patterns
- uses incremental processing and data-quality checks
- produces analytics-ready audience segments
- maps naturally to ADF -> Databricks -> marketing/product consumption flows

Quick run instructions (Databricks)

1. Import the notebook into your Databricks workspace (Workspace -> Import).
2. Attach the notebook to a cluster (Databricks Runtime 11.x+ recommended).
3. Run cells in order. Key cells:
   - Cell 3: setup & imports
   - Cell 5: sample data creation
   - Cell 8: write/read aggregated Delta
   - Cell 11: CDC MERGE example
   - Cell 13: data-quality checks
   - Cell 16: dbutils parameterization

Quick run instructions (local companion)

Install dependencies:

```bash
python3 -m pip install -r audience_intelligence_requirements.txt
```

Run the local demo:

```bash
python3 audience_intelligence_local_demo.py
```

Optional custom output directory:

```bash
python3 audience_intelligence_local_demo.py --output-dir tmp/audience_demo
```

This writes:
- `fan_360.parquet`
- `segment_summary.csv`

Paths

The notebook writes to `/tmp/yash_demo/...`.