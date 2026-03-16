#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

try:
    from rdflib import Graph
    from rdflib.term import URIRef
    from rdflib.util import guess_format
except ModuleNotFoundError as exc:
    if exc.name == "rdflib":
        print(
            "[FATAL] Missing dependency: rdflib. Install it with: pip install rdflib",
            file=sys.stderr,
        )
        raise SystemExit(2)
    raise

try:
    from tqdm.auto import tqdm
except ModuleNotFoundError as exc:
    if exc.name == "tqdm":
        print(
            "[FATAL] Missing dependency: tqdm. Install it with: pip install tqdm",
            file=sys.stderr,
        )
        raise SystemExit(2)
    raise


FORMAT_BY_SUFFIX = {
    ".nt": "nt",
    ".n3": "n3",
    ".ttl": "turtle",
    ".rdf": "xml",
    ".owl": "xml",
    ".xml": "xml",
    ".jsonld": "json-ld",
    ".nq": "nquads",
    ".trig": "trig",
}


@dataclass
class FolderResult:
    folder: Path
    files_seen: int = 0
    files_parsed: int = 0
    errors: int = 0
    triples: int = 0
    skipped_no_input: bool = False


def detect_rdf_format(file_path: Path) -> str | None:
    lower_name = file_path.name.lower()

    for suffix, rdf_format in FORMAT_BY_SUFFIX.items():
        if lower_name.endswith(suffix):
            return rdf_format

    guessed = guess_format(str(file_path))
    return guessed


def iter_rdf_files(dataset_dir: Path, recursive: bool, output_name: str) -> Iterable[Path]:
    iterator = dataset_dir.rglob("*") if recursive else dataset_dir.glob("*")

    for file_path in iterator:
        if not file_path.is_file():
            continue
        if file_path.name == output_name:
            continue
        if file_path.name.startswith("."):
            continue

        rdf_format = detect_rdf_format(file_path)
        if rdf_format is not None:
            yield file_path


def log(message: str) -> None:
    tqdm.write(message)


def normalize_uri_text(value: str) -> str:
    return quote(value, safe=":/?#[]@!$&'()*+,;=%-._~")


def sanitize_graph_uris(graph: Graph) -> int:
    rewritten = 0
    triples = list(graph)

    for subject, predicate, obj in triples:
        new_subject = URIRef(normalize_uri_text(str(subject))) if isinstance(subject, URIRef) else subject
        new_predicate = URIRef(normalize_uri_text(str(predicate))) if isinstance(predicate, URIRef) else predicate
        new_object = URIRef(normalize_uri_text(str(obj))) if isinstance(obj, URIRef) else obj

        if (new_subject, new_predicate, new_object) == (subject, predicate, obj):
            continue

        graph.remove((subject, predicate, obj))
        graph.add((new_subject, new_predicate, new_object))
        rewritten += 1

    return rewritten


def convert_dataset_folder(
    dataset_dir: Path,
    output_name: str,
    recursive: bool,
    overwrite: bool,
    fail_fast: bool,
) -> FolderResult:
    result = FolderResult(folder=dataset_dir)
    output_file = dataset_dir / output_name

    if output_file.exists() and not overwrite:
        log(f"[SKIP] {dataset_dir}: {output_name} already exists (use --overwrite)")
        return result

    rdf_files = list(iter_rdf_files(dataset_dir, recursive=recursive, output_name=output_name))
    result.files_seen = len(rdf_files)

    if not rdf_files:
        result.skipped_no_input = True
        log(f"[SKIP] {dataset_dir}: no RDF source files found")
        return result

    graph = Graph()

    file_progress = tqdm(
        sorted(rdf_files),
        desc=f"{dataset_dir.name}",
        unit="file",
        leave=False,
    )
    for rdf_file in file_progress:
        rdf_format = detect_rdf_format(rdf_file)
        if rdf_format is None:
            continue

        try:
            graph.parse(rdf_file.as_posix(), format=rdf_format)
            result.files_parsed += 1
        except Exception as exc:
            result.errors += 1
            log(f"[ERROR] {dataset_dir}: failed to parse {rdf_file.name} ({rdf_format}) -> {exc}")
            if fail_fast:
                raise

    if result.files_parsed == 0:
        log(f"[WARN] {dataset_dir}: no files parsed successfully")
        return result

    rewritten = sanitize_graph_uris(graph)
    if rewritten > 0:
        log(f"[INFO] {dataset_dir}: sanitized {rewritten} triples with invalid URI characters")

    graph.serialize(destination=output_file.as_posix(), format="turtle")
    result.triples = len(graph)

    log(
        f"[OK] {dataset_dir}: wrote {output_name} "
        f"(triples={result.triples}, parsed={result.files_parsed}, errors={result.errors})"
    )
    return result


def dataset_dirs(root: Path) -> list[Path]:
    return sorted(
        d
        for d in root.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Convert each top-level dataset folder into one Turtle file named data.ttl. "
            "All RDF files inside a folder are merged into that output."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root directory containing dataset folders (default: current directory)",
    )
    parser.add_argument(
        "--output-name",
        default="data.ttl",
        help="Output filename for each dataset folder (default: data.ttl)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Also include RDF files in subdirectories of each dataset folder",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop immediately on first parse error",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    root: Path = args.root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"[FATAL] Invalid root directory: {root}", file=sys.stderr)
        return 2

    folders = dataset_dirs(root)
    if not folders:
        print(f"[FATAL] No dataset folders found under {root}", file=sys.stderr)
        return 2

    total_folders = len(folders)
    written_folders = 0
    total_errors = 0

    folder_progress = tqdm(folders, desc="Datasets", unit="dataset")
    for folder in folder_progress:
        try:
            result = convert_dataset_folder(
                dataset_dir=folder,
                output_name=args.output_name,
                recursive=args.recursive,
                overwrite=args.overwrite,
                fail_fast=args.fail_fast,
            )
        except Exception as exc:
            log(f"[FATAL] {folder}: {exc}")
            return 1

        total_errors += result.errors
        if result.files_parsed > 0:
            written_folders += 1

    log("\n=== Summary ===")
    log(f"Folders scanned: {total_folders}")
    log(f"Folders written: {written_folders}")
    log(f"Total parse errors: {total_errors}")

    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
