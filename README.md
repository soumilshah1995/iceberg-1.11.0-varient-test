# Testing Iceberg 1.11.0 Variant Shredding on EMR: Spark Append, Filter, and Agg Benchmark

**Subtitle:** Shredded vs non-shredded Parquet on S3 Tables — write cost vs read win on GitHub Archive JSON

---

## TL;DR

On EMR with **Iceberg 1.11.0** (format v3, VARIANT column), variant shredding trades **2.7× slower writes** for **~34% faster reads** across 21 filter/agg tests. Shred wins **20 of 21** read patterns. Storage is **+20%** (990 vs 3 Parquet columns). The one no-shred win: filter on nested array field `$.payload.commits`.

---

## Setup

| Item | Value |
|------|-------|
| **Iceberg** | `1.11.0` · `iceberg-spark-runtime-4.0_2.13` · format v3 |
| **Catalog** | AWS S3 Tables REST (`s3tablesbucket.test.*`) |
| **Spark** | 4.0.2-amzn-0 on YARN |
| **Cluster** | master `m7g.xlarge` · 3× core `r6g.2xlarge` · 4 executors × 4 cores × 8 GB |
| **Dataset** | [GH Archive 2024-01-15 hour 12](https://data.gharchive.org/2024-01-15-12.json.gz) · ~287k events/load |
| **Schema** | `(id BIGINT, v VARIANT)` |
| **Load** | 5 sequential appends → **1,434,320 rows** · 5 data files each |
| **Read timing** | 5 epochs per test (avg reported) |
| **Tables** | `gh_noshred` (`write.parquet.shred-variants=false`) · `gh_shred` (`=true`) |
| **Query API** | Spark `variant_get(v, '$.path')` |

**Benchmark flow:** 5 appends complete → 12 filter tests → 9 agg tests (each × 5 epochs).

---

## Storage

| Table | Parquet columns (sample file) | Rows | Files | Size |
|-------|------------------------------|------|-------|------|
| `gh_noshred` | 3 (`id`, `v.metadata`, `v.value`) | 1,434,320 | 5 | **0.54 GB** |
| `gh_shred` | ~990 typed sub-paths | 1,434,320 | 5 | **0.65 GB** (+20%) |

---

## Write — append speed

| Test tag | Query | No-shred avg | Shred avg | Shred vs no-shred |
|----------|-------|--------------|-----------|-------------------|
| **WRITE_APPEND** | Append ~287k JSON rows as `(id, v VARIANT)` × 5 runs | **31.26 s** | **84.16 s** | **+169% slower (2.7×)** |

---

## Read — summary by category

| Category | Tests | No-shred avg | Shred avg | Shred vs no-shred | Shred wins |
|----------|-------|--------------|-----------|-------------------|------------|
| **Filter** | 12 | 3.29 s | 2.25 s | **−32% faster** | 11 / 12 |
| **Agg / GROUP BY** | 9 | 3.27 s | 2.09 s | **−36% faster** | 9 / 9 |
| **All reads** | 21 | 3.28 s | 2.18 s | **−34% faster** | **20 / 21** |

Negative % = shred faster.

---

## Filter results (tagged)

| Tag | Variant path / predicate | No-shred (s) | Shred (s) | Shred Δ | Winner |
|-----|------------------------|--------------|-----------|---------|--------|
| **FILTER_LEVEL1** | `$.id` numeric gt `> 1e9` | 3.90 | 2.15 | **−45%** | shred |
| **FILTER_LEVEL2** | `$.type` = PushEvent | 3.31 | 1.76 | **−47%** | shred |
| **FILTER_LEVEL3** | `$.actor.login` IS NOT NULL | 3.05 | 1.74 | **−43%** | shred |
| **FILTER_LEVEL4** | `$.created_at` LIKE `2024-01-15T12:%` | 3.16 | 1.57 | **−50%** | shred |
| **FILTER_LEVEL5** | `$.type` IN (4 event types) | 3.23 | 1.79 | **−45%** | shred |
| **FILTER_LEVEL6** | `$.public` = true (boolean) | 3.21 | 1.86 | **−42%** | shred |
| **FILTER_LEVEL7** | `$.payload.action` IN (opened/created/closed) | 3.20 | 2.29 | **−28%** | shred |
| **FILTER_LEVEL8** | `$.org.login` IS NOT NULL | 3.13 | 1.50 | **−52%** | shred |
| **FILTER_LEVEL9** | `$.payload.ref` IS NOT NULL | 3.07 | 2.18 | **−29%** | shred |
| **FILTER_LEVEL10** | `$.type` = PushEvent AND `$.actor.login` | 3.25 | 2.45 | **−25%** | shred |
| **FILTER_LEVEL11** | `$.repo.name` LIKE `%linux%` OR `%coreutils%` | 3.54 | 2.73 | **−23%** | shred |
| **FILTER_LEVEL12** | `$.payload.commits` IS NOT NULL (array branch) | 3.42 | 4.93 | **+44%** | **no-shred** |

---

## Agg / GROUP BY results (tagged)

| Tag | Query pattern | No-shred (s) | Shred (s) | Shred Δ | Winner |
|-----|---------------|--------------|-----------|---------|--------|
| **AGG_LEVEL1** | `SUM($.id)` | 3.13 | 1.51 | **−52%** | shred |
| **AGG_LEVEL2** | `GROUP BY $.type` LIMIT 10 | 3.40 | 2.05 | **−40%** | shred |
| **AGG_LEVEL3** | `GROUP BY $.repo.name` LIMIT 10 | 3.62 | 3.01 | **−17%** | shred |
| **AGG_LEVEL4** | `GROUP BY $.payload.action` LIMIT 20 | 3.23 | 2.35 | **−27%** | shred |
| **AGG_LEVEL5** | `GROUP BY $.org.login` LIMIT 20 | 3.23 | 1.71 | **−47%** | shred |
| **AGG_LEVEL6** | `MIN($.id)`, `MAX($.id)` | 3.12 | 1.48 | **−53%** | shred |
| **AGG_LEVEL7** | `COUNT DISTINCT $.type` | 3.26 | 1.90 | **−42%** | shred |
| **AGG_LEVEL8** | `AVG($.payload.size)` on PushEvent | 3.18 | 2.69 | **−15%** | shred |
| **AGG_COMPLEX** | filter 3 types + GROUP BY + COUNT DISTINCT actor | 3.24 | 2.09 | **−35%** | shred |

---

## Findings

| Finding | Detail |
|---------|--------|
| **Write tax** | Shredding costs **~2.7×** append time — materializing ~990 typed columns per file |
| **Read reward** | Filters and aggs on variant paths are **~34% faster** on average with shred |
| **Best shred gains** | Numeric aggs on `$.id` (−52 to −53%), string/date filters (−43 to −50%) |
| **Only shred loss** | `FILTER_LEVEL12` on `$.payload.commits` (+44%) — nested array / complex branch |
| **Storage** | +20% bytes for typed sub-columns |

---

## Example queries (Spark SQL)

```sql
-- FILTER_LEVEL2: string eq on variant
SELECT COUNT(*) FROM s3tablesbucket.test.gh_shred
WHERE CAST(variant_get(v, '$.type') AS STRING) = '"PushEvent"';

-- AGG_LEVEL6: numeric min/max
SELECT MIN(CAST(variant_get(v, '$.id') AS BIGINT)),
       MAX(CAST(variant_get(v, '$.id') AS BIGINT))
FROM s3tablesbucket.test.gh_shred;

-- FILTER_LEVEL12: only pattern where no-shred won
SELECT COUNT(*) FROM s3tablesbucket.test.gh_noshred
WHERE variant_get(v, '$.payload.commits') IS NOT NULL;
```

---

## Recommendation

Enable **`write.parquet.shred-variants=true`** on EMR/Iceberg when tables are **read-heavy** (filters + aggs on JSON paths) and you can pay the write + storage premium. Keep no-shred for **write-heavy** pipelines or workloads heavy on **array/complex nested** fields until you validate those paths.

**Harness:** `run.py` · **Raw output:** `repo/results`
