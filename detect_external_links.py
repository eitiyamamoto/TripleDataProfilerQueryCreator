#!/usr/bin/env python3
"""Detect external-link predicates from TTL metadata by authority.

This script reads VoID/SBM metadata in Turtle format and flags predicates whose
objectClass authority is not in the local authority set derived from the dataset.
It does not rely on namespaces and treats all predicates as potential links.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

import rdflib
from rdflib import Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
VOID = Namespace("http://rdfs.org/ns/void#")
SBM = Namespace("http://sparqlbuilder.org/2015/09/rdf-metadata-schema#")


def _authority_from_uri(uri: str) -> Optional[str]:
    parsed = urlparse(uri)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return parsed.netloc.lower()
    return None


def _authority_from_term(term) -> Optional[str]:
    if isinstance(term, URIRef):
        return _authority_from_uri(str(term))
    if isinstance(term, rdflib.term.Literal):
        return _authority_from_uri(str(term))
    return None


def collect_local_authorities(graph: rdflib.Graph, mode: str) -> Set[str]:
    authorities: Set[str] = set()

    for endpoint in graph.objects(None, SD.endpoint):
        auth = _authority_from_term(endpoint)
        if auth:
            authorities.add(auth)

    if mode in {"endpoint+name", "endpoint+name+urispace", "full"}:
        for name in graph.objects(None, SD.name):
            auth = _authority_from_term(name)
            if auth:
                authorities.add(auth)

    if mode in {"endpoint+name+urispace", "full"}:
        for uri_space in graph.objects(None, VOID.uriSpace):
            auth = _authority_from_term(uri_space)
            if auth:
                authorities.add(auth)

    if mode == "full":
        for class_uri in graph.objects(None, VOID["class"]):
            auth = _authority_from_term(class_uri)
            if auth:
                authorities.add(auth)

    return authorities


@dataclass
class PredicateStats:
    predicate: str
    object_class_authorities: Set[str] = field(default_factory=set)
    external_authorities: Set[str] = field(default_factory=set)
    missing_object_class: int = 0
    total_triples: int = 0
    literal_triples: int = 0
    subject_authority_only: bool = False
    missing_in_original_ttl: bool = False
    source: str = "ttl_metadata"

    def as_dict(self, literal_only_mode: bool = False) -> Dict:
        self.subject_authority_only = self.total_triples > 0 and self.literal_triples == self.total_triples

        # Determine if mapping is possible based on mode
        if literal_only_mode:
            # If there are any non-literal objects, there's a possibility of external mapping
            possible_mapping = self.literal_triples < self.total_triples
        else:
            possible_mapping = bool(self.external_authorities)
        
        # Determine status
        if possible_mapping:
            status = "possible-mapping"
        elif self.missing_object_class > 0:
            status = "undetermined"
        else:
            status = "not-mapping"
        
        return {
            "predicate": self.predicate,
            "object_class_authorities": sorted(self.object_class_authorities),
            "external_authorities": sorted(self.external_authorities),
            "missing_object_class": self.missing_object_class,
            "total_triples": self.total_triples,
            "literal_triples": self.literal_triples,
            "subject_authority_only": self.subject_authority_only,
            "missing_in_original_ttl": self.missing_in_original_ttl,
            "source": self.source,
            "possible_mapping": possible_mapping,
            "status": status,
        }


def analyze_predicates(graph: rdflib.Graph, local_authorities: Set[str]) -> List[PredicateStats]:
    stats_map: Dict[str, PredicateStats] = {}

    literal_classes = {RDFS.Literal, RDF.langString, XSD.string}

    for dataset in graph.subjects(VOID.propertyPartition, None):
        for partition in graph.objects(dataset, VOID.propertyPartition):
            predicate = graph.value(partition, VOID.property)
            if not isinstance(predicate, URIRef):
                continue
            predicate_str = str(predicate)
            stats = stats_map.setdefault(predicate_str, PredicateStats(predicate=predicate_str))

            relations = list(graph.objects(partition, SBM.classRelation))
            if not relations:
                stats.missing_object_class += 1
                continue

            for rel in relations:
                # Extract void:triples value from the classRelation
                triples_literal = graph.value(rel, VOID.triples)
                triples_count = 0
                if triples_literal is not None:
                    try:
                        triples_count = int(triples_literal)
                        stats.total_triples += triples_count
                    except (ValueError, TypeError):
                        pass

                obj_class = graph.value(rel, SBM.objectClass)
                if obj_class in literal_classes:
                    stats.literal_triples += triples_count
                    continue
                auth = _authority_from_term(obj_class) if obj_class is not None else None
                if not auth:
                    stats.missing_object_class += 1
                    continue
                stats.object_class_authorities.add(auth)
                if auth not in local_authorities:
                    stats.external_authorities.add(auth)

    return list(stats_map.values())


def _extract_endpoint_from_graph(graph: rdflib.Graph) -> Optional[str]:
        for endpoint in graph.objects(None, SD.endpoint):
                if isinstance(endpoint, URIRef):
                        return str(endpoint)
        return None


def fetch_endpoint_predicates(
        endpoint: str,
        timeout: int = 30,
        include_named_graphs: bool = True,
) -> Set[str]:
        predicates: Set[str] = set()

        if include_named_graphs:
                query = """
                SELECT DISTINCT ?p WHERE {
                    {
                        ?s ?p ?o .
                    }
                    UNION
                    {
                        GRAPH ?g {
                            ?s ?p ?o .
                        }
                    }
                    FILTER(isIRI(?p))
                }
                """
        else:
                query = """
                SELECT DISTINCT ?p WHERE {
                    ?s ?p ?o .
                    FILTER(isIRI(?p))
                }
                """

        sparql = SPARQLWrapper(endpoint)
        sparql.setReturnFormat(SPARQL_JSON)
        sparql.setTimeout(timeout)
        sparql.setQuery(query)

        results = sparql.queryAndConvert()
        for binding in results.get("results", {}).get("bindings", []):
                predicate = binding.get("p", {}).get("value")
                if predicate:
                        predicates.add(predicate)

        return predicates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect external-link predicates in VoID/SBM Turtle metadata.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to TTL metadata file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write JSON results",
    )
    parser.add_argument(
        "--local-authority-mode",
        choices=["endpoint", "endpoint+name", "endpoint+name+urispace", "full"],
        default="endpoint",
        help=(
            "How to derive local authorities: endpoint (default), endpoint+name, "
            "endpoint+name+urispace, or full (includes class URIs)."
        ),
    )
    parser.add_argument(
        "--literal-only-mode",
        action="store_true",
        help=(
            "Use only the presence of literal triples to decide if a predicate can have "
            "a mapping to an outside source, ignoring external authorities."
        ),
    )
    parser.add_argument(
        "--discover-missing-predicates",
        action="store_true",
        help=(
            "Query the endpoint for DISTINCT predicates and add predicates that are missing "
            "from the original TTL as possible mapping candidates."
        ),
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="SPARQL endpoint URL override for missing predicate discovery.",
    )
    parser.add_argument(
        "--endpoint-timeout",
        type=int,
        default=30,
        help="Timeout (seconds) for endpoint predicate discovery queries (default: 30).",
    )
    parser.add_argument(
        "--default-graph-only",
        action="store_true",
        help="When discovering missing predicates, query only the default graph.",
    )
    args = parser.parse_args()

    graph = rdflib.Graph()
    graph.parse(str(args.input), format="turtle")

    local_authorities = collect_local_authorities(graph, args.local_authority_mode)
    predicate_stats = analyze_predicates(graph, local_authorities)

    discovered_endpoint_predicates: List[str] = []
    missing_predicates_added = 0

    if args.discover_missing_predicates:
        endpoint = args.endpoint or _extract_endpoint_from_graph(graph)
        if not endpoint:
            print(
                "Error: --discover-missing-predicates requires --endpoint or sd:endpoint in the TTL.",
                file=sys.stderr,
            )
            return 1

        try:
            endpoint_predicates = fetch_endpoint_predicates(
                endpoint=endpoint,
                timeout=args.endpoint_timeout,
                include_named_graphs=not args.default_graph_only,
            )
            ttl_predicates = {s.predicate for s in predicate_stats}
            missing_predicates = sorted(endpoint_predicates - ttl_predicates)

            for predicate in missing_predicates:
                predicate_stats.append(
                    PredicateStats(
                        predicate=predicate,
                        missing_in_original_ttl=True,
                        source="endpoint_scan",
                    )
                )

            missing_predicates_added = len(missing_predicates)
            discovered_endpoint_predicates = sorted(endpoint_predicates)
        except Exception as exc:
            print(f"Error discovering missing predicates from endpoint: {exc}", file=sys.stderr)
            return 1

    result = {
        "input": str(args.input),
        "local_authority_mode": args.local_authority_mode,
        "literal_only_mode": args.literal_only_mode,
        "discover_missing_predicates": args.discover_missing_predicates,
        "local_authorities": sorted(local_authorities),
        "endpoint": args.endpoint or _extract_endpoint_from_graph(graph),
        "endpoint_predicates_count": len(discovered_endpoint_predicates),
        "missing_predicates_added": missing_predicates_added,
        "predicate_stats": [s.as_dict(literal_only_mode=args.literal_only_mode) for s in sorted(predicate_stats, key=lambda x: x.predicate)],
    }

    output = json.dumps(result, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())