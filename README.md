# largerdfbench

Docker-first workspace for running LargeRDFBench datasets locally on Virtuoso and generating/executing optimized federated SPARQL queries.

## What this project contains

- **13 local SPARQL endpoints** via Docker Compose (`8887` to `8899`), one per dataset.
- **Query analysis pipeline**: extract triple patterns + enrich with tripleprofile metadata.
- **Federated query optimizer**: generate SERVICE-based federated queries and reports.
- **Query runner**: execute optimized queries and save JSON results.
- **Precomputed artifacts** under `optimization/` and `query_results/`.

## Repository layout

- `docker-compose.yml`: Virtuoso services and dataset mounts.
- `sparql_triple_extractor.py`: builds `*.analysis.json` from benchmark queries.
- `federated_query_optimizer.py`: builds `*.optimization.json` and `*.report.txt`.
- `run_optimized_queries.py`: executes optimized queries and stores results.
- `tripleprofile/`: predicate metadata used by extractor/optimizer.
- `optimization/`: generated analysis/optimization artifacts.
- `query_results/`: per-query execution outputs and run summaries.
- `LargeRDFBench/`: original benchmark materials and query set.

## Prerequisites

- Docker Desktop (Compose v2)
- Python 3.10+ (3.11+ recommended)
- macOS note: images are configured with `platform: linux/amd64`, so Apple Silicon uses emulation.

Install Python dependencies for the analysis pipeline:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install rdflib tqdm
```

## Quick start (Docker data loading path)

Start all benchmark endpoints:

```bash
docker compose up -d
```

Start only selected endpoints (example):

```bash
docker compose up -d linkedtcga-m drugbank kegg
```

Check service status:

```bash
docker compose ps
```

Check one endpoint responds:

```bash
curl -s "http://localhost:8892/sparql/?query=ASK%20%7B%20?s%20?p%20?o%20%7D&format=text%2Fplain"
```

## Endpoint map

| Dataset | Host endpoint | Compose service |
|---|---|---|
| LinkedTCGA-M | `http://localhost:8887/sparql/` | `linkedtcga-m` |
| LinkedTCGA-E | `http://localhost:8888/sparql/` | `linkedtcga-e` |
| LinkedTCGA-A | `http://localhost:8889/sparql/` | `linkedtcga-a` |
| ChEBI | `http://localhost:8890/sparql/` | `chebi` |
| DBPedia-Subset | `http://localhost:8891/sparql/` | `dbpedia-subset` |
| DrugBank | `http://localhost:8892/sparql/` | `drugbank` |
| GeoNames | `http://localhost:8893/sparql/` | `geonames` |
| Jamendo | `http://localhost:8894/sparql/` | `jamendo` |
| KEGG | `http://localhost:8895/sparql/` | `kegg` |
| LMDB | `http://localhost:8896/sparql/` | `lmdb` |
| NYT | `http://localhost:8897/sparql/` | `nyt` |
| SWDFood | `http://localhost:8898/sparql/` | `swdfood` |
| Affymetrix | `http://localhost:8899/sparql/` | `affymetrix` |

## Query analysis and optimization

### ロジック概要

このリポジトリの処理は、基本的に以下の 3 段階で動きます。

1. **トリプル抽出（`sparql_triple_extractor.py`）**
  - SPARQL を algebra に変換して、`BGP`/`Join`/`Union`/`SubSelect`（`ToMultiSet`）を再帰走査します。
  - トリプルはスコープ木（root・UNION分岐・subquery）として保持し、`scope_tree` に保存します。
  - 述語ごとに `tripleprofile/*.ttl` を参照して、`endpoint`・`subject_class`・`object_class`・`triple_count`・authority 情報を付与します。
  - 最後に、抽出した述語のうちメタデータが付与できた割合を `metadata_coverage` として計算します。

2. **連合クエリ最適化（`federated_query_optimizer.py`）**
  - `*.analysis.json` を入力に、述語メタデータから到達可能エンドポイントを推定します。
  - 可能なトリプルを endpoint 単位に束ねて `SERVICE` 句を構築し、`optimized_federated_query` を生成します。
  - 推定不能なトリプルは `unknown_endpoint_triples` に残し、最適化状態（例: partial）を出力します。

3. **実行（`run_optimized_queries.py`）**
  - `*.optimization.json` を読み込み、`SERVICE <...>` を抽出して事前チェックを行います。
  - `--service-endpoint-mode` に応じて、実行前に SERVICE URL を書き換えます。
    - `none`: 変更なし
    - `host.docker.internal`: `localhost` を `host.docker.internal` に変換
    - `docker-service`: `localhost:889x` を compose サービス名（`drugbank` など）+ `:8890` に変換
  - クエリは `--submit-endpoint` に POST し、JSON レスポンスを保存します。失敗時はリトライ設定に従って再試行します。

補足:
- 実行結果行数は SPARQL JSON の `results.bindings` 件数で算出します。
- バッチ実行時は query ごとの詳細と全体集計を `run_summary.json` に保存します。

### 1) Extract triples and metadata from queries

Run for all benchmark query files:

```bash
python3 sparql_triple_extractor.py \
  --queries-dir ./LargeRDFBench/BigRDFBench-Utilities/queries \
  --output-dir ./optimization \
  --tripleprofile-dir ./tripleprofile
```

Run for one query file (example `C1`):

```bash
python3 sparql_triple_extractor.py \
  --query-file ./LargeRDFBench/BigRDFBench-Utilities/queries/C1 \
  --output-dir ./optimization
```

### 2) Generate optimized federated queries

Run for all analysis files:

```bash
python3 federated_query_optimizer.py \
  --analysis-dir ./optimization \
  --output-dir ./optimization
```

Run for one analysis file:

```bash
python3 federated_query_optimizer.py \
  --analysis-file ./optimization/C1.analysis.json \
  --output-dir ./optimization
```

## Run optimized queries

Example execution for `C1`:

```bash
python3 run_optimized_queries.py \
  --query-id C1 \
  --optimization-dir ./optimization \
  --output-dir ./query_results \
  --submit-endpoint http://localhost:8892/sparql/ \
  --service-endpoint-mode docker-service
```

Useful options:

- `--service-endpoint-mode none`: keep SERVICE URLs unchanged.
- `--service-endpoint-mode host.docker.internal`: rewrite localhost SERVICE hosts.
- `--service-endpoint-mode docker-service`: rewrite to compose DNS names (`drugbank`, `kegg`, ...).
- `--dry-run`: validate files without HTTP execution.

## Outputs

- `optimization/*.analysis.json`: extracted triples + metadata coverage.
- `optimization/*.optimization.json`: optimized federated query payloads.
- `optimization/*.report.txt`: human-readable optimization reports.
- `optimization/extractor_run_summary.json`: extractor batch summary.
- `optimization/optimizer_run_summary.json`: optimizer batch summary.
- `query_results/*.results.json`: execution results per query.
- `query_results/run_summary.json`: execution batch summary.

## Current status and caveats

- The current batch summaries report **32 queries processed** in extractor/optimizer runs.
- Current optimization summaries show **partial** optimization status for existing artifacts.
- Query execution requires endpoints to allow federated `SERVICE` execution in Virtuoso.
- This README intentionally documents the Docker Compose workflow as the primary loading path.

## Shutdown

```bash
docker compose down
```

## Reference

For original benchmark background, datasets, and publication context, see:

- `LargeRDFBench/README.md`
- `federated_query_creation_guide.md`
