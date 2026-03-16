#!/usr/bin/env python3

import argparse
import json
import re
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


DATASET_PORTS = {
    "LinkedTCGA-M": 8887,
    "LinkedTCGA-E": 8888,
    "LinkedTCGA-A": 8889,
    "ChEBI": 8890,
    "DBPedia-Subset": 8891,
    "DrugBank": 8892,
    "GeoNames": 8893,
    "Jamendo": 8894,
    "KEGG": 8895,
    "LMDB": 8896,
    "NYT": 8897,
    "SWDFood": 8898,
    "Affymetrix": 8899,
}
PORT_TO_DATASET = {port: name for name, port in DATASET_PORTS.items()}
PORT_TO_SERVICE = {
    8887: "linkedtcga-m",
    8888: "linkedtcga-e",
    8889: "linkedtcga-a",
    8890: "chebi",
    8891: "dbpedia-subset",
    8892: "drugbank",
    8893: "geonames",
    8894: "jamendo",
    8895: "kegg",
    8896: "lmdb",
    8897: "nyt",
    8898: "swdfood",
    8899: "affymetrix",
}


@dataclass
class OptimizationQuery:
    query_id: str
    source_file: Path
    optimized_query: str
    optimization_status: str
    unknown_endpoint_triples: int
    endpoints_used: list[str]


@dataclass
class ServiceRewrite:
    old_endpoint: str
    new_endpoint: str
    mode: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run optimized federated SPARQL queries from optimization JSON files.",
        epilog=(
            "Notes: this submits SPARQL 1.1 queries to a single endpoint, which must be "
            "allowed to perform federated SERVICE calls. In Virtuoso, ensure required "
            "SPARQL permissions are granted and do not set a default named graph in UI."
        ),
    )
    parser.add_argument(
        "--optimization-dir",
        type=Path,
        default=Path("optimization"),
        help="Directory containing *.optimization.json files (default: optimization)",
    )
    parser.add_argument(
        "--query-id",
        action="append",
        dest="query_ids",
        help="Run one query id (e.g., C1). Can be repeated.",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        action="append",
        dest="input_files",
        help="Run a specific optimization JSON file. Can be repeated.",
    )
    parser.add_argument(
        "--submit-endpoint",
        default="http://localhost:8892/sparql/",
        help="SPARQL endpoint to receive federated query text (default: http://localhost:8892/sparql/)",
    )
    parser.add_argument(
        "--service-endpoint-mode",
        choices=["none", "host.docker.internal", "docker-service"],
        default="none",
        help=(
            "Rewrite SERVICE URLs before execution: "
            "none=keep as-is, host.docker.internal=replace localhost with host.docker.internal, "
            "docker-service=replace localhost:889x with compose service DNS at port 8890"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("query_results"),
        help="Directory to save per-query JSON results and run summary (default: query_results)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout per query in seconds (default: 120)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Retry count for transient HTTP/network failures (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only load and validate query files; do not execute HTTP requests.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop batch execution on first failed query.",
    )
    return parser.parse_args()


def collect_input_files(
    optimization_dir: Path, query_ids: list[str] | None, input_files: list[Path] | None
) -> list[Path]:
    files: list[Path] = []

    if input_files:
        files.extend(input_files)
    elif query_ids:
        for query_id in query_ids:
            files.append(optimization_dir / f"{query_id}.optimization.json")
    else:
        files.extend(sorted(optimization_dir.glob("*.optimization.json")))

    seen: set[Path] = set()
    deduped: list[Path] = []
    for file_path in files:
        resolved = file_path.resolve() if file_path.exists() else file_path
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(file_path)

    return deduped


def load_optimization_query(file_path: Path) -> OptimizationQuery:
    if not file_path.exists():
        raise FileNotFoundError(f"Optimization file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    query_id = payload.get("query_id") or file_path.name.split(".")[0]
    optimized_query = payload.get("optimized_federated_query")
    if not optimized_query or not isinstance(optimized_query, str):
        raise ValueError(f"Missing optimized_federated_query in {file_path}")

    unknown_endpoint_triples = payload.get("unknown_endpoint_triples", [])
    if isinstance(unknown_endpoint_triples, list):
        unknown_count = len(unknown_endpoint_triples)
    elif isinstance(unknown_endpoint_triples, int):
        unknown_count = unknown_endpoint_triples
    else:
        unknown_count = 0

    analysis = payload.get("optimization_analysis", {})
    endpoints_used = analysis.get("endpoints_used", [])
    if not isinstance(endpoints_used, list):
        endpoints_used = []

    return OptimizationQuery(
        query_id=str(query_id),
        source_file=file_path,
        optimized_query=optimized_query,
        optimization_status=str(payload.get("optimization_status", "unknown")),
        unknown_endpoint_triples=unknown_count,
        endpoints_used=[str(item) for item in endpoints_used],
    )


def extract_service_endpoints(sparql: str) -> list[str]:
    matches = re.findall(r"SERVICE\s*<([^>]+)>", sparql, flags=re.IGNORECASE)
    return sorted(set(matches))


def _normalize_service_path(path: str) -> str:
    if not path:
        return "/sparql/"
    if not path.endswith("/"):
        return f"{path}/"
    return path


def rewrite_service_endpoints(
    sparql: str, mode: str
) -> tuple[str, list[ServiceRewrite]]:
    if mode == "none":
        return sparql, []

    rewrites: list[ServiceRewrite] = []

    def replace(match: re.Match[str]) -> str:
        old_endpoint = match.group(1)
        parsed = urlparse(old_endpoint)
        host = (parsed.hostname or "").lower()
        if host not in {"localhost", "127.0.0.1"}:
            return match.group(0)

        if parsed.port is None:
            return match.group(0)

        path = _normalize_service_path(parsed.path)
        scheme = parsed.scheme or "http"

        if mode == "host.docker.internal":
            new_endpoint = f"{scheme}://host.docker.internal:{parsed.port}{path}"
        else:
            service_name = PORT_TO_SERVICE.get(parsed.port)
            if not service_name:
                return match.group(0)
            new_endpoint = f"{scheme}://{service_name}:8890{path}"

        if new_endpoint != old_endpoint:
            rewrites.append(
                ServiceRewrite(
                    old_endpoint=old_endpoint,
                    new_endpoint=new_endpoint,
                    mode=mode,
                )
            )

        return f"SERVICE <{new_endpoint}>"

    updated = re.sub(r"SERVICE\s*<([^>]+)>", replace, sparql, flags=re.IGNORECASE)
    return updated, rewrites


def dataset_label(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    port = parsed.port
    if port is None:
        return "unknown"
    return PORT_TO_DATASET.get(port, "unknown")


def run_query(
    submit_endpoint: str, query_text: str, timeout: float, retries: int
) -> tuple[bool, dict[str, Any], float, str | None]:
    started = time.perf_counter()
    attempts = max(retries, 0) + 1
    last_error = None

    for attempt_index in range(attempts):
        body = urlencode(
            {
                "query": query_text,
                "format": "application/sparql-results+json",
            }
        ).encode("utf-8")

        request = Request(
            submit_endpoint,
            data=body,
            method="POST",
            headers={
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                "User-Agent": "largerdfbench-query-runner/1.0",
            },
        )

        try:
            with urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw)
                elapsed = time.perf_counter() - started
                return True, parsed, elapsed, None
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            last_error = f"HTTP {exc.code} {exc.reason}: {detail[:1200]}"
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = f"Network error: {exc}"
        except json.JSONDecodeError as exc:
            last_error = f"Invalid JSON response: {exc}"
        except Exception as exc:
            last_error = f"Unexpected error: {exc}"

        if attempt_index + 1 < attempts:
            time.sleep(min(2.0, 0.2 * (attempt_index + 1)))

    elapsed = time.perf_counter() - started
    return False, {}, elapsed, last_error


def row_count(results_payload: dict[str, Any]) -> int:
    bindings = results_payload.get("results", {}).get("bindings", [])
    if isinstance(bindings, list):
        return len(bindings)
    return 0


def build_preflight(query_item: OptimizationQuery) -> dict[str, Any]:
    service_endpoints = extract_service_endpoints(query_item.optimized_query)
    endpoint_info = [
        {
            "endpoint": endpoint,
            "dataset": dataset_label(endpoint),
        }
        for endpoint in service_endpoints
    ]
    return {
        "service_endpoints": endpoint_info,
        "metadata_endpoints_used": query_item.endpoints_used,
    }


def main() -> None:
    args = parse_args()

    files = collect_input_files(args.optimization_dir, args.query_ids, args.input_files)
    if not files:
        print("No optimization files found.", file=sys.stderr)
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    run_started = datetime.now(timezone.utc)
    summary: dict[str, Any] = {
        "started_at": run_started.isoformat(),
        "submit_endpoint": args.submit_endpoint,
        "service_endpoint_mode": args.service_endpoint_mode,
        "optimization_dir": str(args.optimization_dir),
        "total_files": len(files),
        "dry_run": args.dry_run,
        "results": [],
    }

    print(f"Loaded {len(files)} optimization file(s)")
    print(f"Submit endpoint: {args.submit_endpoint}")
    print(f"SERVICE endpoint mode: {args.service_endpoint_mode}")
    if args.dry_run:
        print("Dry-run mode: queries will not be executed")

    success_count = 0
    failure_count = 0

    for index, file_path in enumerate(files, start=1):
        print(f"\n[{index}/{len(files)}] Processing {file_path}")
        try:
            query_item = load_optimization_query(file_path)
        except Exception as exc:
            failure_count += 1
            entry = {
                "query_id": file_path.name,
                "source_file": str(file_path),
                "status": "failed_to_load",
                "error": str(exc),
            }
            summary["results"].append(entry)
            print(f"  - Failed to load: {exc}")
            if args.fail_fast:
                break
            continue

        preflight = build_preflight(query_item)
        warnings: list[str] = []

        if query_item.optimization_status.lower() == "partial":
            warnings.append("optimization_status is partial")
        if query_item.unknown_endpoint_triples > 0:
            warnings.append(
                f"unknown_endpoint_triples={query_item.unknown_endpoint_triples}"
            )
        unknown_datasets = [
            item["endpoint"]
            for item in preflight["service_endpoints"]
            if item["dataset"] == "unknown"
        ]
        if unknown_datasets:
            warnings.append(f"unmapped SERVICE endpoints: {', '.join(unknown_datasets)}")

        service_desc = ", ".join(
            f"{item['dataset']} ({item['endpoint']})"
            for item in preflight["service_endpoints"]
        )
        rewritten_query, rewrite_details = rewrite_service_endpoints(
            query_item.optimized_query, args.service_endpoint_mode
        )
        rewritten_service_endpoints = extract_service_endpoints(rewritten_query)

        print(f"  - Query ID: {query_item.query_id}")
        print(f"  - SERVICE endpoints: {service_desc or 'none found'}")
        if rewrite_details:
            for rewrite in rewrite_details:
                print(
                    "  - Rewrite: "
                    f"{rewrite.old_endpoint} -> {rewrite.new_endpoint} ({rewrite.mode})"
                )
        if warnings:
            for warning in warnings:
                print(f"  - Warning: {warning}")

        if args.dry_run:
            entry = {
                "query_id": query_item.query_id,
                "source_file": str(query_item.source_file),
                "status": "dry_run",
                "warnings": warnings,
                "preflight": preflight,
                "service_endpoint_mode": args.service_endpoint_mode,
                "service_rewrites": [
                    {
                        "old_endpoint": rewrite.old_endpoint,
                        "new_endpoint": rewrite.new_endpoint,
                        "mode": rewrite.mode,
                    }
                    for rewrite in rewrite_details
                ],
                "service_endpoints_after_rewrite": rewritten_service_endpoints,
            }
            summary["results"].append(entry)
            success_count += 1
            continue

        ok, payload, elapsed, error_msg = run_query(
            submit_endpoint=args.submit_endpoint,
            query_text=rewritten_query,
            timeout=args.timeout,
            retries=args.retries,
        )

        if ok:
            rows = row_count(payload)
            output_path = args.output_dir / f"{query_item.query_id}.results.json"
            with output_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)

            success_count += 1
            entry = {
                "query_id": query_item.query_id,
                "source_file": str(query_item.source_file),
                "status": "success",
                "row_count": rows,
                "elapsed_seconds": round(elapsed, 3),
                "result_file": str(output_path),
                "warnings": warnings,
                "preflight": preflight,
                "service_endpoint_mode": args.service_endpoint_mode,
                "service_rewrites": [
                    {
                        "old_endpoint": rewrite.old_endpoint,
                        "new_endpoint": rewrite.new_endpoint,
                        "mode": rewrite.mode,
                    }
                    for rewrite in rewrite_details
                ],
                "service_endpoints_after_rewrite": rewritten_service_endpoints,
            }
            summary["results"].append(entry)
            print(f"  - Success: {rows} row(s) in {elapsed:.3f}s")
            print(f"  - Saved: {output_path}")
        else:
            failure_count += 1
            entry = {
                "query_id": query_item.query_id,
                "source_file": str(query_item.source_file),
                "status": "failed",
                "elapsed_seconds": round(elapsed, 3),
                "error": error_msg,
                "warnings": warnings,
                "preflight": preflight,
                "service_endpoint_mode": args.service_endpoint_mode,
                "service_rewrites": [
                    {
                        "old_endpoint": rewrite.old_endpoint,
                        "new_endpoint": rewrite.new_endpoint,
                        "mode": rewrite.mode,
                    }
                    for rewrite in rewrite_details
                ],
                "service_endpoints_after_rewrite": rewritten_service_endpoints,
            }
            summary["results"].append(entry)
            print(f"  - Failed after {elapsed:.3f}s")
            print(f"  - Error: {error_msg}")
            if args.fail_fast:
                break

    run_finished = datetime.now(timezone.utc)
    summary["finished_at"] = run_finished.isoformat()
    summary["success_count"] = success_count
    summary["failure_count"] = failure_count

    summary_path = args.output_dir / "run_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    print("\nRun completed")
    print(f"  - Success: {success_count}")
    print(f"  - Failed:  {failure_count}")
    print(f"  - Summary: {summary_path}")

    if failure_count > 0 and not args.dry_run:
        sys.exit(2)


if __name__ == "__main__":
    main()
