#!/usr/bin/env python3
"""Run the TCGAM authority pipeline with one command.

Pipeline steps:
1) detect_external_links.py
2) query_predicate_authorities.py
3) update_ttl_with_authorities.py

The SPARQL endpoint is discovered from sd:endpoint metadata in the TTL by default.
You may optionally pass --endpoint as an explicit override for both query steps.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run detect/query/update authority pipeline in one command."
    )
    parser.add_argument(
        "--ttl",
        type=Path,
        required=True,
        help="Input TTL file (e.g., largerdfbench/tcgam.ttl)",
    )
    parser.add_argument(
        "--endpoint",
        help="Optional SPARQL endpoint URL override (default: discover from TTL metadata)",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use (default: current interpreter)",
    )
    parser.add_argument(
        "--predicates-json",
        type=Path,
        help="Output JSON for detected predicates (default: auto-derived from TTL path)",
    )
    parser.add_argument(
        "--mappings-json",
        type=Path,
        help="Output JSON for predicate authorities (default: auto-derived from TTL path)",
    )
    parser.add_argument(
        "--output-ttl",
        type=Path,
        help="Output updated TTL file (default: auto-derived from TTL path)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of predicates to query in parallel in step 2 (default: 10)",
    )
    parser.add_argument(
        "--page-workers",
        type=int,
        default=5,
        help="Maximum number of page offsets to query in parallel per predicate in step 2 (default: 5)",
    )
    return parser.parse_args()


def derive_paths(ttl_path: Path, args: argparse.Namespace) -> tuple[Path, Path, Path]:
    """Derive predicates, mappings, and output paths from TTL filename."""
    base_dir = ttl_path.parent
    stem = ttl_path.stem  # filename without extension
    
    predicates_json = args.predicates_json or (base_dir / "predicates" / f"{stem}.json")
    mappings_json = args.mappings_json or (base_dir / "mappings" / f"{stem}.json")
    output_ttl = args.output_ttl or (base_dir / f"{stem}_update.ttl")
    
    return predicates_json, mappings_json, output_ttl


def resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return SCRIPT_DIR / path


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def run_step(name: str, cmd: list[str]) -> None:
    printable = " ".join(str(part) for part in cmd)
    print(f"\n[{name}] {printable}")
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    if result.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}")


def count_mapping_errors(mappings_path: Path) -> int:
    data: Any = json.loads(mappings_path.read_text(encoding="utf-8"))
    results = data.get("results", []) if isinstance(data, dict) else []

    error_count = 0
    for item in results:
        if isinstance(item, dict) and item.get("error"):
            error_count += 1
    return error_count


def main() -> int:
    args = parse_args()

    ttl_path = resolve_path(args.ttl)
    
    if not ttl_path.exists():
        print(f"Error: TTL file not found: {ttl_path}", file=sys.stderr)
        return 1
    
    predicates_json_path, mappings_json_path, output_ttl_path = derive_paths(ttl_path, args)
    
    # Resolve relative paths
    predicates_json_path = resolve_path(predicates_json_path)
    mappings_json_path = resolve_path(mappings_json_path)
    output_ttl_path = resolve_path(output_ttl_path)

    ensure_parent(predicates_json_path)
    ensure_parent(mappings_json_path)
    ensure_parent(output_ttl_path)

    detect_cmd = [
        args.python,
        str(SCRIPT_DIR / "detect_external_links.py"),
        "--input",
        str(ttl_path),
        "--output",
        str(predicates_json_path),
        "--literal-only-mode",
        "--discover-missing-predicates",
    ]

    if args.endpoint:
        detect_cmd.extend(["--endpoint", args.endpoint])

    query_cmd = [
        args.python,
        str(SCRIPT_DIR / "query_predicate_authorities.py"),
        str(predicates_json_path),
        "--output",
        str(mappings_json_path),
        "--ttl-file",
        str(ttl_path),
        "--max-workers",
        str(args.max_workers),
        "--page-workers",
        str(args.page_workers),
    ]

    if args.endpoint:
        query_cmd.extend(["--endpoint", args.endpoint])

    update_cmd = [
        args.python,
        str(SCRIPT_DIR / "update_ttl_with_authorities.py"),
        str(ttl_path),
        str(mappings_json_path),
        str(output_ttl_path),
    ]

    try:
        run_step("1/3 detect_external_links", detect_cmd)

        if not predicates_json_path.exists():
            raise RuntimeError(
                f"Step 1 completed but predicates JSON was not created: {predicates_json_path}"
            )

        run_step("2/3 query_predicate_authorities", query_cmd)

        if not mappings_json_path.exists():
            raise RuntimeError(
                f"Step 2 completed but mappings JSON was not created: {mappings_json_path}"
            )

        error_count = count_mapping_errors(mappings_json_path)
        if error_count > 0:
            raise RuntimeError(
                f"Step 2 produced {error_count} predicate error(s) in mappings JSON; aborting in strict mode"
            )

        run_step("3/3 update_ttl_with_authorities", update_cmd)

        if not output_ttl_path.exists():
            raise RuntimeError(
                f"Step 3 completed but output TTL was not created: {output_ttl_path}"
            )

    except RuntimeError as exc:
        print(f"\nPipeline failed: {exc}", file=sys.stderr)
        return 1

    print("\nPipeline completed successfully.")
    print(f"Predicates JSON: {predicates_json_path}")
    print(f"Mappings JSON:   {mappings_json_path}")
    print(f"Updated TTL:     {output_ttl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
