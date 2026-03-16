#!/usr/bin/env python3
"""
SPARQL Triple Pattern Extractor using rdflib
Loads SPARQL queries and extracts all triple patterns with scope grouping
"""

import os
from pathlib import Path
from rdflib.plugins.sparql import prepareQuery
from rdflib import Graph, Namespace
import json
import argparse
import re
from urllib.parse import urlparse


class PredicateMetadataLoader:
    """Load and query predicate metadata from TTL triple profile files"""
    
    def __init__(self, tripleprofile_dir="./tripleprofile"):
        """
        Initialize metadata loader by loading all TTL files from tripleprofile directory
        
        Args:
            tripleprofile_dir: Path to directory containing TTL metadata files
        """
        self.tripleprofile_dir = Path(tripleprofile_dir)
        self.predicate_index = {}  # {predicate_uri: [(endpoint, subject_class, object_class, triple_count), ...]}
        self.endpoint_graphs = {}  # {endpoint: Graph}
        
        # Define namespaces for querying TTL files
        self.VOID = Namespace("http://rdfs.org/ns/void#")
        self.SBM = Namespace("http://sparqlbuilder.org/2015/09/rdf-metadata-schema#")
        self.SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
        self.RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        
        self._load_metadata()
    
    def _load_metadata(self):
        """Load all TTL files and build predicate index"""
        if not self.tripleprofile_dir.exists():
            print(f"Warning: tripleprofile directory not found: {self.tripleprofile_dir}")
            return
        
        # Find all TTL files
        ttl_files = list(self.tripleprofile_dir.glob("*.ttl"))
        
        if not ttl_files:
            print(f"Warning: No TTL files found in {self.tripleprofile_dir}")
            return
        
        print(f"Loading {len(ttl_files)} TTL metadata files...")
        
        for ttl_file in ttl_files:
            try:
                self._load_ttl_file(ttl_file)
            except Exception as e:
                print(f"Warning: Error loading {ttl_file.name}: {e}")
        
        print(f"Loaded metadata for {len(self.predicate_index)} unique predicates")
    
    def _load_ttl_file(self, ttl_file):
        """Load a single TTL file and extract predicate metadata"""
        graph = Graph()
        graph.parse(str(ttl_file), format="turtle")
        
        # Query for endpoint URL
        endpoint_query = """
            PREFIX sd: <http://www.w3.org/ns/sparql-service-description#>
            SELECT ?endpoint
            WHERE {
                ?service a sd:Service ;
                    sd:endpoint ?endpoint .
            }
        """
        
        endpoint = None
        for row in graph.query(endpoint_query):
            endpoint = str(row.endpoint)
            break
        
        if not endpoint:
            print(f"Warning: No endpoint found in {ttl_file.name}")
            return
        
        self.endpoint_graphs[endpoint] = graph

        authority_by_predicate = self._extract_authorities(graph)
        
        # Query for predicate metadata
        metadata_query = """
            PREFIX void: <http://rdfs.org/ns/void#>
            PREFIX sbm: <http://sparqlbuilder.org/2015/09/rdf-metadata-schema#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?predicate ?subjectClass ?objectClass ?tripleCount
            WHERE {
                ?partition a void:Dataset ;
                    void:property ?predicate .
                
                OPTIONAL {
                    ?partition void:triples ?partitionTriples .
                }
                
                OPTIONAL {
                    ?partition sbm:classRelation ?relation .
                    ?relation sbm:subjectClass ?subjectClass ;
                              sbm:objectClass ?objectClass .
                    
                    OPTIONAL {
                        ?relation void:triples ?tripleCount .
                    }
                }
            }
        """
        
        for row in graph.query(metadata_query):
            predicate = str(row.predicate)
            subject_class = str(row.subjectClass) if row.subjectClass else None
            object_class = str(row.objectClass) if row.objectClass else None
            triple_count = int(row.tripleCount) if row.tripleCount else None
            predicate_authorities = authority_by_predicate.get(
                predicate,
                {'subject': set(), 'object': set()}
            )
            
            metadata = {
                'endpoint': endpoint,
                'subject_class': subject_class,
                'object_class': object_class,
                'triple_count': triple_count,
                'authorities': {
                    'subject': sorted(predicate_authorities['subject']),
                    'object': sorted(predicate_authorities['object'])
                }
            }
            
            if predicate not in self.predicate_index:
                self.predicate_index[predicate] = []
            
            self.predicate_index[predicate].append(metadata)

    def _extract_authorities(self, graph):
        """Extract authority domains per predicate and relation type."""
        authority_query = """
            PREFIX void: <http://rdfs.org/ns/void#>
            PREFIX sbm: <http://sparqlbuilder.org/2015/09/rdf-metadata-schema#>

            SELECT ?predicate ?authority ?relationType
            WHERE {
                ?partition a void:Dataset ;
                    void:property ?predicate ;
                    sbm:authorityRelation ?authRelation .

                ?authRelation sbm:authority ?authority ;
                              sbm:relationType ?relationType .
            }
        """

        authority_by_predicate = {}

        for row in graph.query(authority_query):
            predicate = str(row.predicate)
            authority = self._normalize_authority(row.authority)
            relation_type = str(row.relationType).lower()

            if predicate not in authority_by_predicate:
                authority_by_predicate[predicate] = {'subject': set(), 'object': set()}

            if not authority:
                continue

            if relation_type.endswith('subject'):
                authority_by_predicate[predicate]['subject'].add(authority)
            elif relation_type.endswith('object'):
                authority_by_predicate[predicate]['object'].add(authority)

        return authority_by_predicate

    def _normalize_authority(self, authority_term):
        """Normalize authority URI to scheme+host form for matching."""
        authority = str(authority_term)
        if authority.startswith('<') and authority.endswith('>'):
            authority = authority[1:-1]

        parsed = urlparse(authority)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return None
    
    def _normalize_predicate_uri(self, predicate):
        """Normalize predicate URI by stripping n3 formatting"""
        # Remove angle brackets from n3 format if present
        if predicate.startswith('<') and predicate.endswith('>'):
            return predicate[1:-1]
        return predicate
    
    def lookup_predicate_metadata(self, predicate):
        """
        Look up metadata for a predicate URI
        
        Args:
            predicate: Predicate URI (can be in n3 format like <http://...> or plain URI)
        
        Returns:
            List of metadata dicts with keys: endpoint, subject_class, object_class, triple_count
            Returns empty list if predicate not found
        """
        # Normalize the predicate URI
        normalized = self._normalize_predicate_uri(predicate)
        
        # Look up in index
        return self.predicate_index.get(normalized, [])


class ScopeNode:
    """Represents a scope group (UNION branch, subquery, etc.)"""
    def __init__(self, scope_type="root", label=None):
        self.scope_type = scope_type  # 'root', 'union_branch', 'subquery', 'join', etc.
        self.label = label  # e.g. 'LEFT', 'RIGHT' for union branches
        self.triples = []
        self.children = []  # For nested scopes
        self.predicates = set()
        self.subjects = set()
        self.objects = set()
        self.variables = set()
    
    def to_dict(self):
        """Convert scope node to dictionary for JSON serialization"""
        result = {
            'scope_type': self.scope_type,
            'triples': self.triples,
            'num_triples': len(self.triples),
            'unique_predicates': sorted(list(self.predicates)),
            'subjects': sorted(list(self.subjects)),
            'objects': sorted(list(self.objects)),
            'variables': sorted(list(self.variables)),
        }
        if self.label:
            result['label'] = self.label
        if self.children:
            result['children'] = [child.to_dict() for child in self.children]
        return result


class SPARQLTripleExtractor:
    """Extract triple patterns from SPARQL queries using rdflib with scope preservation"""
    
    def __init__(self, tripleprofile_dir="./tripleprofile"):
        """
        Initialize triple extractor with metadata loader
        
        Args:
            tripleprofile_dir: Path to directory containing TTL metadata files
        """
        self.root_scope = ScopeNode(scope_type="root")
        self.current_scope = None
        self.predicates = set()
        self.subjects = set()
        self.objects = set()
        self.variables = set()
        self.query_modifiers = {'distinct': False, 'reduced': False}
        
        # Initialize metadata loader
        self.metadata_loader = PredicateMetadataLoader(tripleprofile_dir)
    
    def extract_from_query(self, sparql_query):
        """
        Extract triple patterns from a SPARQL query string with scope grouping
        Returns a dict with grouped extracted information
        """
        self.root_scope = ScopeNode(scope_type="root")
        self.current_scope = self.root_scope
        self.predicates = set()
        self.subjects = set()
        self.objects = set()
        self.variables = set()
        self.query_modifiers = self._extract_select_modifiers(sparql_query)
        
        try:
            # Parse the SPARQL query
            query = prepareQuery(sparql_query)
            
            # Extract from algebra tree
            self._extract_from_algebra(query.algebra, self.current_scope)
            
        except Exception as e:
            import traceback
            print(f"Error parsing query: {e}")
            traceback.print_exc()
            return None
        
        return self._format_results()
    
    def _extract_from_algebra(self, algebra_node, scope):
        """Recursively extract triple patterns from algebra node, preserving scope boundaries"""
        if algebra_node is None:
            return
        
        node_type = algebra_node.__class__.__name__
        print(f"Processing algebra node type: {node_type}")
        
        # Handle CompValue nodes (generic algebra containers)
        if node_type == 'CompValue':
            # Get the operation name
            op_name = getattr(algebra_node, 'name', None)
            print(f"  CompValue operation: {op_name}")
            
            # Handle BGP (Basic Graph Pattern)
            if op_name == 'BGP':
                triples = getattr(algebra_node, 'triples', None)
                if triples is not None:
                    for triple in triples:
                        self._process_triple(triple, scope)
            
            # Handle Join operations
            elif op_name == 'Join':
                p1 = getattr(algebra_node, 'p1', None)
                p2 = getattr(algebra_node, 'p2', None)
                if p1:
                    self._extract_from_algebra(p1, scope)
                if p2:
                    self._extract_from_algebra(p2, scope)
            
            # Handle Union operations - create separate branch scopes
            elif op_name == 'Union':
                p1 = getattr(algebra_node, 'p1', None)
                p2 = getattr(algebra_node, 'p2', None)
                
                if p1:
                    left_scope = ScopeNode(scope_type="union_branch", label="LEFT")
                    scope.children.append(left_scope)
                    self._extract_from_algebra(p1, left_scope)
                
                if p2:
                    right_scope = ScopeNode(scope_type="union_branch", label="RIGHT")
                    scope.children.append(right_scope)
                    self._extract_from_algebra(p2, right_scope)
            
            # Handle subqueries (SubSelect becomes ToMultiSet in rdflib)
            elif op_name in ('SubSelect', 'ToMultiSet'):
                subquery_scope = ScopeNode(scope_type="subquery")
                scope.children.append(subquery_scope)
                
                # Process subquery content
                p = getattr(algebra_node, 'p', None)
                if p:
                    self._extract_from_algebra(p, subquery_scope)
            
            # Handle other operations (Project, Filter, Distinct, OrderBy, etc.)
            else:
                # Try common attributes - process child operations in current scope
                p = getattr(algebra_node, 'p', None)
                if p:
                    self._extract_from_algebra(p, scope)
                
                p1 = getattr(algebra_node, 'p1', None)
                if p1:
                    self._extract_from_algebra(p1, scope)
                
                p2 = getattr(algebra_node, 'p2', None)
                if p2:
                    self._extract_from_algebra(p2, scope)
        
        # Handle legacy BGP nodes (if any)
        elif node_type == 'BGP':
            if hasattr(algebra_node, 'triples') and algebra_node.triples is not None:
                for triple in algebra_node.triples:
                    self._process_triple(triple, scope)
    
    def _process_triple(self, triple, scope):
        """Process and store a single triple pattern in the given scope"""
        s, p, o = triple
        
        triple_str = (self._term_to_str(s), self._term_to_str(p), self._term_to_str(o))
        scope.triples.append(triple_str)
        
        # Track predicates at scope level
        pred_str = self._term_to_str(p)
        scope.predicates.add(pred_str)
        self.predicates.add(pred_str)
        
        # Track subjects and objects at scope level
        subj_str = self._term_to_str(s)
        scope.subjects.add(subj_str)
        self.subjects.add(subj_str)
        
        obj_str = self._term_to_str(o)
        scope.objects.add(obj_str)
        self.objects.add(obj_str)
        
        # Track variables at scope level
        if str(s).startswith('?'):
            scope.variables.add(str(s))
            self.variables.add(str(s))
        if str(p).startswith('?'):
            scope.variables.add(str(p))
            self.variables.add(str(p))
        if str(o).startswith('?'):
            scope.variables.add(str(o))
            self.variables.add(str(o))
    
    def _term_to_str(self, term):
        """Convert RDF term to string representation"""
        if hasattr(term, 'n3'):
            return term.n3()
        return str(term)

    def _extract_select_modifiers(self, sparql_query):
        """Extract top-level SELECT modifiers from the original query text."""
        query_no_comments = re.sub(r"#.*$", "", sparql_query, flags=re.MULTILINE)
        compact_query = " ".join(query_no_comments.split())
        match = re.search(r"\bSELECT\b\s*(DISTINCT|REDUCED)?", compact_query, flags=re.IGNORECASE)

        if not match:
            return {'distinct': False, 'reduced': False}

        modifier = (match.group(1) or '').upper()
        return {
            'distinct': modifier == 'DISTINCT',
            'reduced': modifier == 'REDUCED'
        }
    
    def _format_results(self):
        """Format extracted information as grouped structure (grouped-only output)"""
        results = {
            'scope_tree': self.root_scope.to_dict(),
            'total_triples': self._count_all_triples(self.root_scope),
            'unique_predicates': sorted(list(self.predicates)),
            'subjects': sorted(list(self.subjects)),
            'objects': sorted(list(self.objects)),
            'variables': sorted(list(self.variables)),
            'num_unique_predicates': len(self.predicates),
            'query_modifiers': self.query_modifiers
        }
        
        # Enrich with predicate metadata
        self._enrich_triple_patterns(results)
        
        return results
    
    def _enrich_triple_patterns(self, results):
        """Enrich results with predicate metadata from TTL files"""
        predicate_metadata = {}
        endpoints_found = set()
        
        # Look up metadata for each unique predicate
        for predicate in self.predicates:
            metadata_list = self.metadata_loader.lookup_predicate_metadata(predicate)
            
            if metadata_list:
                predicate_metadata[predicate] = metadata_list
                
                # Collect unique endpoints
                for metadata in metadata_list:
                    if metadata.get('endpoint'):
                        endpoints_found.add(metadata['endpoint'])
        
        # Add to results
        results['predicate_metadata'] = predicate_metadata
        results['endpoints_found'] = sorted(list(endpoints_found))
        results['num_endpoints'] = len(endpoints_found)
    
    def _count_all_triples(self, scope):
        """Count total triples in scope tree (including children)"""
        count = len(scope.triples)
        for child in scope.children:
            count += self._count_all_triples(child)
        return count
    
    def load_query_file(self, file_path):
        """Load SPARQL query from file"""
        try:
            with open(file_path, 'r') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return None
    
    def extract_from_file(self, file_path):
        """Load and extract triples from query file"""
        query = self.load_query_file(file_path)
        if query:
            return self.extract_from_query(query)
        return None
    
    def extract_from_directory(self, directory_path):
        """Extract triples from all query files in a directory"""
        results = {}
        query_dir = Path(directory_path)
        
        for query_file in sorted(query_dir.iterdir()):
            if query_file.is_file() and not query_file.name.startswith('.'):
                query_name = query_file.name
                result = self.extract_from_file(str(query_file))
                if result:
                    results[query_name] = result
                    print(f"✓ Extracted from {query_name}")
        
        return results


def _compute_metadata_coverage(result):
    """Compute metadata coverage summary for extracted predicates"""
    unique_predicates = result.get('unique_predicates', [])
    predicate_metadata = result.get('predicate_metadata', {})
    covered_predicates = sorted(predicate_metadata.keys())
    missing_predicates = sorted([p for p in unique_predicates if p not in predicate_metadata])

    total_predicates = len(unique_predicates)
    covered_count = len(covered_predicates)
    coverage_ratio = (covered_count / total_predicates) if total_predicates else 0.0

    return {
        'total_predicates': total_predicates,
        'covered_predicates': covered_count,
        'missing_predicates': missing_predicates,
        'coverage_ratio': round(coverage_ratio, 4),
        'is_partial': len(missing_predicates) > 0
    }


def _write_json(file_path, data):
    """Write JSON data to file path"""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)


def main():
    """Run extractor for a single query or all queries in a directory"""
    parser = argparse.ArgumentParser(description="Extract SPARQL triple patterns with tripleprofile metadata")
    parser.add_argument(
        '--query-file',
        help='Path to a single SPARQL query file'
    )
    parser.add_argument(
        '--queries-dir',
        default='./LargeRDFBench/BigRDFBench-Utilities/queries',
        help='Directory containing query files for batch extraction'
    )
    parser.add_argument(
        '--output-dir',
        default='./optimization',
        help='Output directory for per-query analysis files'
    )
    parser.add_argument(
        '--tripleprofile-dir',
        default='./tripleprofile',
        help='Directory containing triple profile TTL files'
    )
    parser.add_argument(
        '--legacy-output',
        action='store_true',
        help='Also write legacy root output files for compatibility'
    )
    args = parser.parse_args()

    extractor = SPARQLTripleExtractor(tripleprofile_dir=args.tripleprofile_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.query_file:
        query_path = Path(args.query_file)
        if not query_path.exists() or not query_path.is_file():
            print(f"Query file not found: {query_path}")
            raise SystemExit(1)

        query_id = query_path.name
        print("=" * 60)
        print(f"Extracting triples from {query_id} with metadata enrichment...")
        print("=" * 60)
        result = extractor.extract_from_file(str(query_path))
        if not result:
            print(f"Failed to extract triples from {query_id}")
            raise SystemExit(1)

        result['query_id'] = query_id
        result['query_file'] = str(query_path)
        result['metadata_coverage'] = _compute_metadata_coverage(result)

        analysis_file = output_dir / f"{query_id}.analysis.json"
        _write_json(analysis_file, result)
        print(f"✓ Analysis saved to {analysis_file}")

        if args.legacy_output:
            _write_json('query_analysis_with_metadata.json', result)
            print("✓ Legacy analysis saved to query_analysis_with_metadata.json")

        print(f"Total triples: {result['total_triples']}")
        print(f"Unique predicates: {result['num_unique_predicates']}")
        print(f"Endpoints found: {result['num_endpoints']}")
        print(f"Metadata coverage: {result['metadata_coverage']['covered_predicates']}/{result['metadata_coverage']['total_predicates']}")
        return

    queries_dir = Path(args.queries_dir)
    if not queries_dir.exists() or not queries_dir.is_dir():
        print(f"Queries directory not found: {queries_dir}")
        raise SystemExit(1)

    query_files = [
        query_file for query_file in sorted(queries_dir.iterdir())
        if query_file.is_file() and not query_file.name.startswith('.')
    ]

    print("=" * 60)
    print(f"Extracting triples from {len(query_files)} query files...")
    print("=" * 60)

    run_summary = {
        'queries_dir': str(queries_dir),
        'output_dir': str(output_dir),
        'total_queries': len(query_files),
        'processed': 0,
        'failed': 0,
        'partial_metadata': 0,
        'queries': []
    }

    for query_file in query_files:
        query_id = query_file.name
        entry = {
            'query_id': query_id,
            'query_file': str(query_file),
            'status': 'failed'
        }

        try:
            result = extractor.extract_from_file(str(query_file))
            if not result:
                entry['error'] = 'Extraction returned no result'
                run_summary['failed'] += 1
            else:
                result['query_id'] = query_id
                result['query_file'] = str(query_file)
                result['metadata_coverage'] = _compute_metadata_coverage(result)

                analysis_file = output_dir / f"{query_id}.analysis.json"
                _write_json(analysis_file, result)

                entry['status'] = 'ok'
                entry['analysis_file'] = str(analysis_file)
                entry['total_triples'] = result.get('total_triples', 0)
                entry['num_endpoints'] = result.get('num_endpoints', 0)
                entry['metadata_partial'] = result['metadata_coverage']['is_partial']
                entry['missing_predicates'] = result['metadata_coverage']['missing_predicates']

                run_summary['processed'] += 1
                if entry['metadata_partial']:
                    run_summary['partial_metadata'] += 1
                print(f"✓ {query_id} -> {analysis_file.name}")
        except Exception as e:
            entry['error'] = str(e)
            run_summary['failed'] += 1
            print(f"✗ {query_id} -> {e}")

        run_summary['queries'].append(entry)

    summary_file = output_dir / 'extractor_run_summary.json'
    _write_json(summary_file, run_summary)

    if args.legacy_output:
        legacy_results = {}
        for item in run_summary['queries']:
            if item['status'] == 'ok':
                analysis_file = item.get('analysis_file')
                if analysis_file and Path(analysis_file).exists():
                    with open(analysis_file, 'r') as f:
                        legacy_results[item['query_id']] = json.load(f)
        _write_json('query_triples.json', legacy_results)
        print("✓ Legacy batch output saved to query_triples.json")

    print("=" * 60)
    print("Extraction complete")
    print(f"Processed: {run_summary['processed']}/{run_summary['total_queries']}")
    print(f"Failed: {run_summary['failed']}")
    print(f"Partial metadata: {run_summary['partial_metadata']}")
    print(f"Run summary: {summary_file}")
    print("=" * 60)


def _print_scope_tree(scope, indent=0):
    """Pretty-print scope tree structure"""
    prefix = "  " * indent
    scope_type = scope.get('scope_type', 'unknown')
    label = scope.get('label', '')
    num_triples = scope.get('num_triples', 0)
    
    label_str = f" [{label}]" if label else ""
    print(f"{prefix}{scope_type}{label_str}: {num_triples} triples")
    
    if scope.get('triples'):
        for triple in scope['triples']:
            print(f"{prefix}  - {triple}")
    
    if scope.get('children'):
        for child in scope['children']:
            _print_scope_tree(child, indent + 1)


if __name__ == '__main__':
    main()
