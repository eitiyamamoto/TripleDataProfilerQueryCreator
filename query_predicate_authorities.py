#!/usr/bin/env python3
"""Query subject and object authorities for all predicates in predicate_stats.

This script:
1. Loads a JSON mapping file from the mappings directory
2. Queries authorities for every predicate in predicate_stats
3. Executes SPARQL queries to get subject and object authority patterns
4. Saves results as JSON with authority distribution data
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
from collections import Counter
from urllib.parse import urlparse

from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
import rdflib
from rdflib import Namespace
from tqdm import tqdm

SD = Namespace("http://www.w3.org/ns/sparql-service-description#")


def extract_endpoint_from_ttl(ttl_path: str) -> Optional[str]:
    """Extract SPARQL endpoint URL from TTL metadata file."""
    try:
        g = rdflib.Graph()
        g.parse(ttl_path, format="turtle")
        
        # Find the first sd:endpoint
        for endpoint in g.objects(None, SD.endpoint):
            return str(endpoint)
    except Exception as e:
        print(f"Warning: Could not extract endpoint from TTL: {e}", file=sys.stderr)
    
    return None


def find_named_graphs(ttl_file: Optional[Path] = None) -> List[str]:
    """
    Read a TTL file and return named graph URIs declared in Service Description.
    
    Detects named graphs using the SPARQL Service Description vocabulary (sd:namedGraph).
    Returns a list of named graph URIs.
    """
    named_graphs = []
    
    if ttl_file is None or not ttl_file.exists():
        return named_graphs
    
    try:
        g = rdflib.Graph()
        g.parse(ttl_file, format="turtle")
        
        # Find all sd:NamedGraph definitions
        for named_graph_node in g.subjects(rdflib.RDF.type, SD.NamedGraph):
            # Get the sd:name of each named graph
            for graph_name in g.objects(named_graph_node, SD.name):
                graph_name_str = str(graph_name)
                named_graphs.append(graph_name_str)
        
    except Exception as e:
        print(f"Warning: Could not parse {ttl_file}: {e}", file=sys.stderr)
    
    return sorted(set(named_graphs))


def graph_contains_predicate(endpoint: str, predicate: str, graph_uri: str, timeout: int = 30) -> bool:
        """Check quickly whether a named graph has at least one triple with the predicate."""
        query = f"""
        ASK WHERE {{
            GRAPH <{graph_uri}> {{
                ?s <{predicate}> ?o .
            }}
        }}
        """

        try:
                sparql = SPARQLWrapper(endpoint)
                sparql.setReturnFormat(SPARQL_JSON)
                sparql.setTimeout(timeout)
                sparql.setQuery(query)
                results = sparql.queryAndConvert()
                return bool(results.get("boolean"))
        except Exception as e:
                print(f"Warning: Could not verify predicate in named graph {graph_uri}: {e}", file=sys.stderr)
                return False


def query_predicate_authorities(
    endpoint: str, 
    predicate: str, 
    limit: int = 1000,
    timeout: int = 30,
    named_graph: Optional[str] = None,
    show_progress: bool = True,
    page_workers: int = 5,
) -> Dict[str, List[Dict[str, int]]]:
    """
    Query the SPARQL endpoint for subject and object authority patterns using pagination.
    
    If named_graph is provided, queries that specific named graph.
    Otherwise queries the default graph.
    
    First queries for a COUNT to determine total triples, then paginates through results
    with a progress bar using tqdm.
    
    Returns a dict with 'subject_authorities' and 'object_authorities' keys,
    each containing a list of {authority: str, count: int} dicts.
    """
    subject_authorities = Counter()
    object_authorities = Counter()
    total_count = 0
    
    try:
        # First, get the COUNT of matching triples
        if named_graph:
            count_query = f"""
            SELECT (COUNT(*) as ?count) WHERE {{
              GRAPH <{named_graph}> {{
                ?s <{predicate}> ?o .
                FILTER ISIRI(?s)
              }}
            }}
            """
        else:
            count_query = f"""
            SELECT (COUNT(*) as ?count) WHERE {{
              ?s <{predicate}> ?o .
              FILTER ISIRI(?s)
            }}
            """
        
        sparql = SPARQLWrapper(endpoint)
        sparql.setReturnFormat(SPARQL_JSON)
        sparql.setTimeout(timeout)
        sparql.setQuery(count_query)
        
        count_results = sparql.queryAndConvert()
        if count_results["results"]["bindings"]:
            total_count = int(count_results["results"]["bindings"][0]["count"]["value"])
        
        if total_count == 0:
            return {
                "subject_authorities": [],
                "object_authorities": [],
                "total_results": 0
            }
        
        # Calculate number of pages needed
        num_pages = math.ceil(total_count / limit)
        
        if page_workers < 1:
            page_workers = 1

        def fetch_page(page_idx: int) -> tuple[Counter, Counter]:
            offset = page_idx * limit

            if named_graph:
                # Query with GRAPH clause for specific named graph
                query = f"""
                SELECT ?ps ?po WHERE {{
                  GRAPH <{named_graph}> {{
                    ?s <{predicate}> ?o .
                    FILTER ISIRI(?s)
                    
                    BIND(REPLACE(STR(?s), "(https?://[^/]+).*", "$1") AS ?ps)
                    BIND(IF(ISIRI(?o), REPLACE(STR(?o), "(https?://[^/]+).*", "$1"), "any") AS ?po)
                  }}
                }}
                LIMIT {limit}
                OFFSET {offset}
                """
            else:
                # Query default graph (no GRAPH clause)
                query = f"""
                SELECT ?ps ?po WHERE {{
                  ?s <{predicate}> ?o .
                  FILTER ISIRI(?s)
                  
                  BIND(REPLACE(STR(?s), "(https?://[^/]+).*", "$1") AS ?ps)
                  BIND(IF(ISIRI(?o), REPLACE(STR(?o), "(https?://[^/]+).*", "$1"), "any") AS ?po)
                }}
                LIMIT {limit}
                OFFSET {offset}
                """

            sparql = SPARQLWrapper(endpoint)
            sparql.setReturnFormat(SPARQL_JSON)
            sparql.setTimeout(timeout)
            sparql.setQuery(query)

            results = sparql.queryAndConvert()
            bindings = results["results"]["bindings"]

            page_subject_authorities = Counter()
            page_object_authorities = Counter()

            for binding in bindings:
                if "ps" in binding:
                    ps = binding["ps"]["value"]
                    page_subject_authorities[ps] += 1

                if "po" in binding:
                    po = binding["po"]["value"]
                    page_object_authorities[po] += 1

            return page_subject_authorities, page_object_authorities

        page_indices = range(num_pages)
        effective_workers = min(page_workers, num_pages)

        if effective_workers == 1:
            for page_idx in tqdm(
                page_indices,
                desc=f"Querying predicate {predicate}",
                disable=not show_progress,
            ):
                page_subject_authorities, page_object_authorities = fetch_page(page_idx)
                subject_authorities.update(page_subject_authorities)
                object_authorities.update(page_object_authorities)
        else:
            with ThreadPoolExecutor(max_workers=effective_workers) as page_executor:
                page_futures = {
                    page_executor.submit(fetch_page, page_idx): page_idx
                    for page_idx in page_indices
                }

                for future in tqdm(
                    as_completed(page_futures),
                    total=len(page_futures),
                    desc=f"Querying predicate {predicate}",
                    disable=not show_progress,
                ):
                    page_subject_authorities, page_object_authorities = future.result()
                    subject_authorities.update(page_subject_authorities)
                    object_authorities.update(page_object_authorities)
        
        return {
            "subject_authorities": [
                {"authority": auth, "count": count} 
                for auth, count in subject_authorities.most_common()
            ],
            "object_authorities": [
                {"authority": auth, "count": count} 
                for auth, count in object_authorities.most_common()
            ],
            "total_results": total_count
        }
    
    except Exception as e:
        graph_info = f" (named graph: {named_graph})" if named_graph else " (default graph)"
        print(f"Error querying predicate {predicate}{graph_info}: {e}", file=sys.stderr)
        return {
            "subject_authorities": [],
            "object_authorities": [],
            "total_results": 0,
            "error": str(e)
        }


def process_mapping_file(
    mapping_path: Path,
    endpoint: Optional[str] = None,
    limit: int = 1000,
    timeout: int = 30,
    output_path: Optional[Path] = None,
    ttl_file: Optional[Path] = None,
    named_graph: Optional[str] = None,
    default_only: bool = False,
    max_workers: int = 10,
    show_inner_progress: bool = True,
    page_workers: int = 5,
) -> None:
    """Process a mapping JSON file and query authorities for possible mappings.
    
    If named_graph is provided, queries only that specific named graph.
    If default_only is true, queries only the default graph.
    Otherwise, queries the default graph first, then checks the specified TTL file
    for named graphs containing the predicate and queries those as well.
    """
    
    # Load the mapping file
    with open(mapping_path, 'r') as f:
        mapping_data = json.load(f)
    
    # Try to extract endpoint if not provided
    if endpoint is None:
        if "input" in mapping_data:
            ttl_path = mapping_data["input"]
            endpoint = extract_endpoint_from_ttl(ttl_path)
        
        if endpoint is None:
            print("Error: No endpoint provided and could not extract from TTL", 
                  file=sys.stderr)
            sys.exit(1)
    
    print(f"Using SPARQL endpoint: {endpoint}")
    print(f"Processing mapping file: {mapping_path}")
    if named_graph:
        print(f"Querying specific named graph: {named_graph}")
    elif default_only:
        print("Querying default graph only")
    elif ttl_file:
        print(f"Checking for named graphs in: {ttl_file}")
    
    predicates_to_query = list(mapping_data.get("predicate_stats", []))
    candidate_named_graphs: List[str] = []
    if not named_graph and not default_only and ttl_file:
        candidate_named_graphs = find_named_graphs(ttl_file)

    print(f"Found {len(predicates_to_query)} predicates to query (all predicates are eligible)")

    if max_workers < 1:
        print("Warning: --max-workers must be >= 1; using 1", file=sys.stderr)
        max_workers = 1

    print(f"Using up to {max_workers} worker(s) for predicate queries")

    def process_single_predicate(pred_stat: Dict) -> Dict:
        predicate = pred_stat["predicate"]

        result = {
            "predicate": predicate,
            "total_triples": pred_stat.get("total_triples", 0),
            "literal_triples": pred_stat.get("literal_triples", 0),
            "missing_in_original_ttl": pred_stat.get("missing_in_original_ttl", False),
            "source": pred_stat.get("source", "ttl_metadata"),
            "query_limit": limit,
            "graphs": {}
        }
        
        if named_graph:
            # Query only the specified named graph
            named_graph_data = query_predicate_authorities(
                endpoint, predicate, limit, timeout, named_graph=named_graph, show_progress=show_inner_progress,
                page_workers=page_workers
            )
            result["graphs"][named_graph] = named_graph_data
        elif default_only:
            # Query only the default graph
            default_graph_data = query_predicate_authorities(
                endpoint, predicate, limit, timeout, named_graph=None, show_progress=show_inner_progress,
                page_workers=page_workers
            )
            result["graphs"]["default"] = default_graph_data
        else:
            # Query the default graph
            default_graph_data = query_predicate_authorities(
                endpoint, predicate, limit, timeout, named_graph=None, show_progress=show_inner_progress,
                page_workers=page_workers
            )
            result["graphs"]["default"] = default_graph_data

            # Keep only named graphs containing the predicate
            named_graphs_found = [
                ng for ng in candidate_named_graphs
                if graph_contains_predicate(endpoint, predicate, ng, timeout=timeout)
            ]

            # Query each named graph
            for ng in named_graphs_found:
                named_graph_data = query_predicate_authorities(
                    endpoint, predicate, limit, timeout, named_graph=ng, show_progress=show_inner_progress,
                    page_workers=page_workers
                )
                result["graphs"][ng] = named_graph_data

        return result

    # Query predicates in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_predicate = {
            executor.submit(process_single_predicate, pred_stat): pred_stat.get("predicate", "<unknown>")
            for pred_stat in predicates_to_query
        }

        for future in tqdm(
            as_completed(future_to_predicate),
            total=len(future_to_predicate),
            desc="Querying predicates",
            unit="predicate",
        ):
            predicate = future_to_predicate[future]
            try:
                result = future.result()
                results.append(result)

                graph_keys = list(result.get("graphs", {}).keys())
                default_graph_data = result.get("graphs", {}).get("default", {})
                n_subj = len(default_graph_data.get("subject_authorities", [])) if default_graph_data else 0
                n_obj = len(default_graph_data.get("object_authorities", [])) if default_graph_data else 0

                if named_graph:
                    named_graph_data = result.get("graphs", {}).get(named_graph, {})
                    n_subj = len(named_graph_data.get("subject_authorities", []))
                    n_obj = len(named_graph_data.get("object_authorities", []))
                    tqdm.write(f"  {predicate}: {n_subj} subj auth, {n_obj} obj auth")
                elif default_only:
                    tqdm.write(f"  {predicate}: {n_subj} subj auth, {n_obj} obj auth")
                else:
                    named_count = max(len(graph_keys) - 1, 0)
                    graph_info = f" + {named_count} named graph(s)" if named_count else ""
                    tqdm.write(f"  {predicate}: {n_subj} subj auth, {n_obj} obj auth{graph_info}")
            except Exception as e:
                print(f"Error processing predicate {predicate}: {e}", file=sys.stderr)
                results.append(
                    {
                        "predicate": predicate,
                        "graphs": {},
                        "error": str(e),
                    }
                )
    
    # Prepare output
    output_data = {
        "mapping_file": str(mapping_path),
        "endpoint": endpoint,
        "query_limit": limit,
        "total_predicates_queried": len(results),
        "results": results
    }
    
    # Save to output file
    if output_path is None:
        output_path = mapping_path.parent / f"{mapping_path.stem}_authorities.json"
    
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Query subject/object authorities for all predicates in predicate_stats"
        )
    )
    parser.add_argument(
        "mapping_file",
        type=Path,
        help="Path to the mapping JSON file"
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        help="SPARQL endpoint URL (if not provided, will try to extract from input TTL)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="LIMIT clause for SPARQL queries (default: 1000)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Query timeout in seconds (default: 30)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file path (default: <mapping_file>_authorities.json)"
    )
    parser.add_argument(
        "--ttl-file",
        type=Path,
        help="TTL file to discover named graph URIs used for predicate authority queries"
    )
    parser.add_argument(
        "--named-graph",
        type=str,
        help="Specific named graph URI to query instead of auto-discovering all named graphs"
    )
    parser.add_argument(
        "--default-only",
        action="store_true",
        help="Query only the default graph and skip named graph discovery/querying"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of predicates to query in parallel (default: 10)",
    )
    parser.add_argument(
        "--page-workers",
        type=int,
        default=5,
        help="Maximum number of page offsets to query in parallel per predicate (default: 5)",
    )
    parser.add_argument(
        "--hide-inner-progress",
        action="store_true",
        help="Disable per-predicate inner tqdm bars (default: show them)",
    )
    
    args = parser.parse_args()
    
    if not args.mapping_file.exists():
        print(f"Error: Mapping file not found: {args.mapping_file}", file=sys.stderr)
        sys.exit(1)
    
    if args.ttl_file and not args.ttl_file.exists():
        print(f"Error: TTL file not found: {args.ttl_file}", file=sys.stderr)
        sys.exit(1)

    if args.named_graph and args.default_only:
        parser.error("--named-graph and --default-only cannot be used together")
    
    process_mapping_file(
        args.mapping_file,
        args.endpoint,
        args.limit,
        args.timeout,
        args.output,
        args.ttl_file,
        args.named_graph,
        args.default_only,
        args.max_workers,
        not args.hide_inner_progress,
        args.page_workers,
    )


if __name__ == "__main__":
    main()
