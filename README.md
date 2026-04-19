## What are we Singing About? (Databricks edition)

This is a new implementation of [a previous project](https://github.com/joelb856/what-are-we-singing-about) to ingest, process, and serve an analysis of the lyics and metadata associated with the [weekly Billboard Hot 100 songs](https://www.billboard.com/charts/hot-100/) using Databricks Free Edition.

Each Tuesday the pipeline:
1. Ingests [the latest Hot 100 songs](https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/recent.json), including [lyrics](https://lyrist.vercel.app/guide) and [metadata](https://www.last.fm/api)
2. Extracts features (language, word count, [affect frequencies](https://pypi.org/project/NRCLex/))
3. Upserts aggregated metrics into a serving table
---

## Architecture

```mermaid
flowchart LR
    Ingestion["Ingestion"]
    Landing["Landing Volume\n(hot100.raw.landing)"]
    Bronze["Bronze Table\n(hot100.raw.bronze)"]
    Silver["Silver Table\n(hot100.processed.silver)"]
    Gold["Gold Table\n(hot100.serving.gold)"]

    Ingestion --> Landing
    Landing -->|Auto Loader| Bronze
    Bronze -->|Preprocessing and\nfeature extraction| Silver
    Silver -->|Weekly aggregates\nmerge into| Gold
```

---

## Components

### Ingestion

_Coming soon_

### `src/setup.py`
One-time setup notebook.

### `src/bronze.py`
Ingests raw JSON files from `hot100.raw.landing` into a Delta table with Auto Loader as a streaming job. Appends additional metadata (`_ingest_time`, `_source_file`).

### `src/silver.py`
Reads from bronze as a stream, explodes each week's JSON into one row per song, and enriches each row with some basic NLP features using a `pandas_udf`:
- **Language detection** with `langdetect`
- **Word count** after basic lyric cleaning
- **Affect frequencies** with the [NRC Emotion Lexicon](https://saifmohammad.com/WebPages/NRC-Emotion-Lexicon.htm).

### `src/gold.py`
Computes weekly aggregates from silver and upserts from a staging table into the serving table with a `MERGE` statement.

### `resources/pipeline.job.yml`
Runs the bronze → silver → gold notebooks in sequence every Tuesday at 8:30am.

### `databricks.yml`
Databricks Asset Bundle definition, including seperate `dev` and `prod` targets.

---

## Getting Started

### Prerequisites
- [Databricks Free Edition](https://www.databricks.com/try-databricks)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) with a profile named `hot100`:

```bash
databricks configure --profile hot100
```

### Deploy the bundle and test in dev
By default, triggers are paused in development mode. To test the pipeline, ensure there is at least one new JSON file in `hot100.raw.landing` and run it manually:

```bash
databricks bundle deploy --profile hot100
databricks bundle run setup --profile hot100
databricks bundle run pipeline --profile hot100
```

There is currently no logging implemented, but you can confirm it worked by running a query e.g.
```
%sql
select * from hot100.serving.gold limit 10;
```

### Deploy to prod
Once you confirm the pipeline works as expected, deploy to prod to enable the time trigger:
```bash
databricks bundle deploy --profile hot100 --target prod
```