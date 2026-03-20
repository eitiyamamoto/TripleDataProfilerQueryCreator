#!/usr/bin/env python3
"""
Federated Query Optimizer

Takes query analysis with metadata and generates optimized federated SPARQL queries
with endpoint-specific SERVICE clauses and cardinality estimation.
"""

import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import math
import argparse
from urllib.parse import urlparse


class FederatedQueryOptimizer:
    """Generate optimized federated queries and estimate result cardinality"""
    
    def __init__(self):
        """Initialize the optimizer"""
        self.query_analysis = None
        self.endpoint_map = defaultdict(list)  # {endpoint: [predicate info]}
        self.predicate_to_endpoints = defaultdict(list)  # {predicate: [endpoint info]}
        self.selective_predicates = []  # Most selective predicates

    def _count_or_inf(self, value) -> float:
        """Return numeric triple count or infinity when unavailable."""
        if isinstance(value, (int, float)):
            return float(value)
        return float('inf')

    def _count_or_zero(self, value) -> float:
        """Return numeric triple count or zero when unavailable."""
        if isinstance(value, (int, float)):
            return float(value)
        return 0.0
        
    def load_query_analysis(self, analysis_data):
        """Load query analysis from dict or file path"""
        if isinstance(analysis_data, str):
            with open(analysis_data, 'r') as f:
                self.query_analysis = json.load(f)
        else:
            self.query_analysis = analysis_data
        
        self._build_endpoint_index()
    
    def _build_endpoint_index(self):
        """Build endpoint and predicate indexes from query analysis"""
        if not self.query_analysis:
            return
        
        metadata = self.query_analysis.get('predicate_metadata', {})
        
        for predicate, endpoint_list in metadata.items():
            for endpoint_info in endpoint_list:
                endpoint = endpoint_info['endpoint']
                triple_count = endpoint_info.get('triple_count', 0)
                subject_class = endpoint_info.get('subject_class')
                object_class = endpoint_info.get('object_class')
                
                self.endpoint_map[endpoint].append({
                    'predicate': predicate,
                    'subject_class': subject_class,
                    'object_class': object_class,
                    'triple_count': triple_count,
                    'authorities': endpoint_info.get('authorities', {'subject': [], 'object': []})
                })
                
                self.predicate_to_endpoints[predicate].append({
                    'endpoint': endpoint,
                    'subject_class': subject_class,
                    'object_class': object_class,
                    'triple_count': triple_count,
                    'authorities': endpoint_info.get('authorities', {'subject': [], 'object': []})
                })
        
        # Sort predicates by selectivity (fewest triples first)
        self._identify_selective_predicates()
    
    def _identify_selective_predicates(self):
        """Identify most selective predicates for query optimization"""
        predicate_cardinalities = []
        
        for predicate, endpoint_list in self.predicate_to_endpoints.items():
            # Use minimum cardinality across all endpoints
            min_cardinality = min(
                [self._count_or_inf(ep.get('triple_count')) for ep in endpoint_list],
                default=float('inf')
            )
            unique_endpoints = {
                ep.get('endpoint') for ep in endpoint_list
                if ep.get('endpoint')
            }
            unique_class_pairs = {
                (ep.get('subject_class'), ep.get('object_class'))
                for ep in endpoint_list
            }
            predicate_cardinalities.append({
                'predicate': predicate,
                'cardinality': min_cardinality,
                'num_endpoints': len(unique_endpoints),
                'num_possible_classes': len(unique_class_pairs)
            })
        
        # Sort by cardinality (most selective first)
        self.selective_predicates = sorted(
            predicate_cardinalities,
            key=lambda x: x['cardinality'] if x['cardinality'] != float('inf') else float('inf')
        )
    
    def generate_federated_query(self, original_query: str, use_service_clauses: bool = True) -> Dict:
        """
        Generate optimized federated query
        
        Args:
            original_query: Original SPARQL query string
            use_service_clauses: Whether to use SERVICE clauses for federation
        
        Returns:
            Dict with optimized_query, optimization_info, cardinality_estimate
        """
        if not self.query_analysis:
            return {'error': 'No query analysis loaded'}
        
        result = {
            'optimization_strategy': 'federated-with-service-clauses' if use_service_clauses else 'union-based',
            'original_query': original_query,
            'endpoints_used': self.query_analysis.get('endpoints_found', []),
            'num_endpoints': self.query_analysis.get('num_endpoints', 0),
            'optimization_details': self._analyze_query_structure(),
            'cardinality_estimate': self._estimate_cardinality(),
            'endpoint_breakdown': self._get_endpoint_breakdown()
        }
        
        if use_service_clauses:
            result['optimized_query'] = self._generate_service_clause_query()
        else:
            result['optimized_query'] = self._generate_union_query()
        
        return result
    
    def _analyze_query_structure(self) -> Dict:
        """Analyze the query's structure and data flow"""
        scope_tree = self.query_analysis.get('scope_tree', {})
        triples = scope_tree.get('triples', [])
        
        analysis = {
            'num_triples': len(triples),
            'num_unique_predicates': self.query_analysis.get('num_unique_predicates', 0),
            'num_unique_subjects': len(self.query_analysis.get('subjects', [])),
            'num_unique_objects': len(self.query_analysis.get('objects', [])),
            'num_variables': len(self.query_analysis.get('variables', [])),
            'selective_predicates': self.selective_predicates[:3],  # Top 3 most selective
            'multi_endpoint_predicates': [
                p for p in self.query_analysis.get('unique_predicates', [])
                if len({
                    ep.get('endpoint') for ep in self.predicate_to_endpoints.get(p, [])
                    if ep.get('endpoint')
                }) > 1
            ]
        }
        
        return analysis
    
    def _estimate_cardinality(self) -> Dict:
        """Estimate maximum cardinality of query results"""
        if not self.predicate_to_endpoints:
            return {
                'estimated_rows': 0,
                'confidence': 'low',
                'method': 'no-metadata'
            }
        
        # Get triple patterns from query
        triples = self.query_analysis.get('scope_tree', {}).get('triples', [])
        
        # Group triples by subject (to find join chains)
        subject_chains = self._identify_join_chains(triples)
        
        # Estimate using join chain analysis
        cardinality = self._estimate_from_join_chains(subject_chains)
        
        return cardinality
    
    def _identify_join_chains(self, triples: List) -> Dict:
        """Identify join chains based on subject-object connections"""
        chains = defaultdict(list)
        
        for i, (s, p, o) in enumerate(triples):
            # Track which triples are connected through variables
            chains[s].append((i, s, p, o))
        
        return chains
    
    def _estimate_from_join_chains(self, chains: Dict) -> Dict:
        """Estimate cardinality from join chains"""
        if not chains:
            return {'estimated_rows': 0, 'confidence': 'low', 'method': 'no-chains'}
        
        # Find the most selective (smallest) chain as starting point
        smallest_chain = None
        smallest_cardinality = float('inf')
        
        for variable, chain_triples in chains.items():
            if not variable.startswith('?'):
                continue
            
            # Estimate this chain's cardinality
            chain_cardinality = self._estimate_chain_cardinality(chain_triples)
            
            if chain_cardinality < smallest_cardinality:
                smallest_cardinality = chain_cardinality
                smallest_chain = (variable, chain_triples)
        
        if smallest_cardinality == float('inf'):
            return {
                'estimated_rows': 0,
                'confidence': 'low',
                'method': 'no-estimates'
            }
        
        # Conservative estimate: use the minimum across bottlenecks
        estimate = max(1, smallest_cardinality)
        
        return {
            'estimated_rows': int(estimate),
            'estimated_rows_readable': self._format_number(estimate),
            'confidence': 'medium',
            'method': 'join-chain-analysis',
            'reasoning': f'Estimated from {len(chains)} join chains'
        }
    
    def _estimate_chain_cardinality(self, chain_triples: List) -> float:
        """Estimate cardinality for a single join chain"""
        if not chain_triples:
            return 0
        
        cardinalities = []
        
        for _, _, p, _ in chain_triples:
            # Get min cardinality for this predicate across endpoints
            endpoints = self.predicate_to_endpoints.get(p, [])
            if endpoints:
                min_card = min([self._count_or_inf(ep.get('triple_count')) for ep in endpoints])
                cardinalities.append(min_card)
        
        if not cardinalities:
            return float('inf')
        
        # Return the minimum cardinality in the chain (bottleneck)
        return min(cardinalities)

    def _estimate_endpoint_retrieved_triples(self) -> Dict[str, Optional[float]]:
        """Estimate retrieved triples per endpoint using query-assigned triples.

        Rule:
        - Assume all query triple patterns match.
        - For each endpoint, use the smallest known triple_count among triples
          assigned to that endpoint.
        - Ignore missing/null/non-numeric triple_count values.
        """
        if not self.query_analysis:
            return {}

        builder = SmartFederatedQueryBuilder(self.query_analysis)
        assigned_triples = builder._assign_endpoints()

        endpoint_min = {}
        for row in assigned_triples:
            endpoint = row.get('endpoint', 'unknown')
            triple_count = row.get('triple_count')

            if not isinstance(triple_count, (int, float)):
                continue

            if endpoint not in endpoint_min:
                endpoint_min[endpoint] = triple_count
            else:
                endpoint_min[endpoint] = min(endpoint_min[endpoint], triple_count)

        return endpoint_min
    
    def _get_endpoint_breakdown(self) -> List[Dict]:
        """Get breakdown of predicates per endpoint"""
        breakdown = []
        endpoint_estimates = self._estimate_endpoint_retrieved_triples()

        all_endpoints = set(self.endpoint_map.keys()) | set(endpoint_estimates.keys())
        
        for endpoint in sorted(all_endpoints):
            predicates = self.endpoint_map[endpoint]
            total_triples = sum(
                p.get('triple_count', 0)
                for p in predicates
                if isinstance(p.get('triple_count'), (int, float))
            )
            endpoint_retrieved_estimate = endpoint_estimates.get(endpoint)
            
            breakdown.append({
                'endpoint': endpoint,
                'num_predicates': len(predicates),
                'total_triples': total_triples,
                'total_triples_readable': self._format_number(total_triples),
                'predicates': [p['predicate'] for p in predicates],
                'estimated_retrieved_triples': endpoint_retrieved_estimate,
                'estimated_retrieved_triples_readable': (
                    self._format_number(endpoint_retrieved_estimate)
                    if endpoint_retrieved_estimate is not None
                    else 'unknown'
                )
            })
        
        return breakdown
    
    def _generate_service_clause_query(self) -> str:
        """Generate query using SERVICE clauses for federation"""
        # Note: This is a template. Real implementation would require
        # parsing the original query structure
        
        query_parts = []
        query_parts.append("# Optimized Federated Query using SERVICE clauses\n")
        query_parts.append("# Each triple pattern is pushed to its most selective endpoint\n\n")
        
        # Sort endpoints by data size (smallest first for faster iteration)
        sorted_endpoints = sorted(
            self.endpoint_map.items(),
            key=lambda x: sum(self._count_or_zero(p.get('triple_count')) for p in x[1])
        )
        
        for endpoint, predicates in sorted_endpoints:
            query_parts.append(f"# Endpoint: {endpoint}\n")
            query_parts.append(f"# Predicates: {len(predicates)}\n")
            
            for pred in predicates[:5]:  # Show first 5
                query_parts.append(
                    f"#   {pred['predicate']} "
                    f"({pred['subject_class']} -> {pred['object_class']}) "
                    f"[{self._format_number(self._count_or_inf(pred.get('triple_count')))} triples]\n"
                )
            
            query_parts.append("\n")
        
        return "".join(query_parts)
    
    def _generate_union_query(self) -> str:
        """Generate query as UNION of endpoint-specific queries"""
        query_parts = []
        query_parts.append("# Optimized Query using UNION\n")
        query_parts.append("# Combines results from different endpoints\n\n")
        
        for i, endpoint in enumerate(sorted(self.endpoint_map.keys())):
            if i > 0:
                query_parts.append("UNION\n")
            query_parts.append(f"# Results from {endpoint}\n")
        
        return "".join(query_parts)
    
    def _format_number(self, n: float) -> str:
        """Format large numbers readably"""
        if n == float('inf'):
            return "unknown"
        if n > 1_000_000:
            return f"{n/1_000_000:.1f}M"
        if n > 1_000:
            return f"{n/1_000:.1f}K"
        return str(int(n))
    
    def generate_optimization_report(self) -> str:
        """Generate a human-readable optimization report"""
        report_lines = []
        
        report_lines.append("=" * 80)
        report_lines.append("FEDERATED QUERY OPTIMIZATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Query Overview
        report_lines.append("QUERY OVERVIEW")
        report_lines.append("-" * 80)
        analysis = self.query_analysis
        report_lines.append(f"Total Triple Patterns: {len(analysis.get('scope_tree', {}).get('triples', []))}")
        report_lines.append(f"Unique Predicates: {analysis.get('num_unique_predicates', 0)}")
        report_lines.append(f"Endpoints Used: {analysis.get('num_endpoints', 0)}")
        report_lines.append("")
        
        # Selectivity Analysis
        report_lines.append("PREDICATE SELECTIVITY (Most Selective First)")
        report_lines.append("-" * 80)
        for i, pred in enumerate(self.selective_predicates[:10], 1):
            report_lines.append(
                f"{i}. {self._shorten_uri(pred['predicate'])}\n"
                f"   Cardinality: {self._format_number(pred['cardinality'])}\n"
                f"   Endpoints: {pred['num_endpoints']}\n"
                f"   Possible Class Pairs: {pred.get('num_possible_classes', 0)}"
            )
        report_lines.append("")
        
        # Endpoint Breakdown
        report_lines.append("ENDPOINT BREAKDOWN")
        report_lines.append("-" * 80)
        for endpoint_info in self._get_endpoint_breakdown():
            report_lines.append(
                f"Endpoint: {endpoint_info['endpoint']}\n"
                f"  Predicates: {endpoint_info['num_predicates']}\n"
                f"  Total Triples: {endpoint_info['total_triples_readable']}\n"
                f"  Estimated Retrieved Triples: "
                f"{endpoint_info['estimated_retrieved_triples_readable']}"
            )
        report_lines.append("")
        
        # Cardinality Estimate
        report_lines.append("ESTIMATED RESULT CARDINALITY")
        report_lines.append("-" * 80)
        card_estimate = self._estimate_cardinality()
        report_lines.append(f"Method: {card_estimate.get('method', 'unknown')}")
        report_lines.append(f"Estimated Rows: {card_estimate.get('estimated_rows_readable', 'unknown')}")
        report_lines.append(f"Confidence: {card_estimate.get('confidence', 'unknown')}")
        if 'reasoning' in card_estimate:
            report_lines.append(f"Reasoning: {card_estimate['reasoning']}")
        report_lines.append("")
        
        # Optimization Recommendations
        report_lines.append("OPTIMIZATION RECOMMENDATIONS")
        report_lines.append("-" * 80)
        report_lines.append(self._generate_recommendations())
        
        return "\n".join(report_lines)
    
    def _shorten_uri(self, uri: str) -> str:
        """Shorten URI for readability"""
        if uri.startswith('<') and uri.endswith('>'):
            uri = uri[1:-1]
        
        # Try to extract local name
        if '#' in uri:
            return uri.split('#')[-1]
        if '/' in uri:
            return uri.split('/')[-1]
        
        return uri[:50] + "..." if len(uri) > 50 else uri
    
    def _generate_recommendations(self) -> str:
        """Generate optimization recommendations"""
        recommendations = []
        
        # Check for highly selective predicates
        if self.selective_predicates:
            most_selective = self.selective_predicates[0]
            recommendations.append(
                f"• Start query execution with '{self._shorten_uri(most_selective['predicate'])}' "
                f"({self._format_number(most_selective['cardinality'])} triples)"
            )
        
        # Check for multi-endpoint predicates
        multi_endpoint = [
            p for p in self.query_analysis.get('unique_predicates', [])
            if len({
                ep.get('endpoint') for ep in self.predicate_to_endpoints.get(p, [])
                if ep.get('endpoint')
            }) > 1
        ]
        if multi_endpoint:
            recommendations.append(
                f"• Use subject/object class constraints to reduce search space "
                f"for {len(multi_endpoint)} predicates that span multiple endpoints"
            )
        
        # Check endpoint count
        if self.query_analysis.get('num_endpoints', 0) > 2:
            recommendations.append(
                f"• Use SERVICE clauses to parallelize execution across "
                f"{self.query_analysis.get('num_endpoints', 0)} endpoints"
            )
        
        if not recommendations:
            recommendations.append("• Query structure is already well-optimized")
        
        return "\n".join(recommendations)


class SmartFederatedQueryBuilder:
    """Build smart federated queries using class constraints"""
    
    def __init__(self, analysis_data):
        """Initialize with query analysis data"""
        self.analysis = analysis_data
        self.triple_patterns = analysis_data.get('scope_tree', {}).get('triples', [])
        self.metadata = analysis_data.get('predicate_metadata', {})
        self.query_modifiers = analysis_data.get('query_modifiers', {})
        if 'distinct' not in self.query_modifiers and 'reduced' not in self.query_modifiers:
            self.query_modifiers = self._load_query_modifiers_from_query_file()
        self.select_variables = self._load_select_variables_from_query_file()
        self.constraint_map = self._build_constraint_map()
        self.inferred_var_class = self._infer_variable_classes()
        self.inferred_var_authority = self._infer_variable_authorities()

    def _load_query_modifiers_from_query_file(self) -> Dict[str, bool]:
        """Backfill query modifiers from original query file when metadata is missing."""
        query_file = self.analysis.get('query_file')
        if not query_file:
            return {'distinct': False, 'reduced': False}

        try:
            query_text = Path(query_file).read_text()
        except OSError:
            return {'distinct': False, 'reduced': False}

        query_upper = " ".join(query_text.split()).upper()
        select_index = query_upper.find('SELECT')
        if select_index < 0:
            return {'distinct': False, 'reduced': False}

        after_select = query_upper[select_index + len('SELECT'):].lstrip()
        return {
            'distinct': after_select.startswith('DISTINCT'),
            'reduced': after_select.startswith('REDUCED')
        }

    def _load_select_variables_from_query_file(self) -> List[str]:
        """Load projected variables from original query file SELECT clause."""
        query_file = self.analysis.get('query_file')
        if not query_file:
            return []

        try:
            query_text = Path(query_file).read_text()
        except OSError:
            return []

        tokens = " ".join(query_text.split()).split()
        if not tokens:
            return []

        upper_tokens = [token.upper() for token in tokens]
        try:
            select_index = upper_tokens.index('SELECT')
        except ValueError:
            return []

        i = select_index + 1
        while i < len(tokens) and upper_tokens[i] in {'DISTINCT', 'REDUCED'}:
            i += 1

        if i >= len(tokens):
            return []

        if tokens[i] == '*':
            return []

        select_vars = []
        while i < len(tokens):
            token = tokens[i]
            token_upper = upper_tokens[i]

            if token_upper == 'WHERE':
                break

            if token.startswith('?'):
                cleaned = token.rstrip(',;')
                if cleaned not in select_vars:
                    select_vars.append(cleaned)

            i += 1

        return select_vars

    def _get_projection_variables(self) -> List[str]:
        """Return SELECT projection variables from query or fallback to inferred vars."""
        if self.select_variables:
            return self.select_variables

        return sorted({
            term for s, _, o in self.triple_patterns for term in (s, o)
            if isinstance(term, str) and term.startswith('?') and term not in {'?enzyme', '?Chemicalreaction'}
        })

    def _build_select_clause(self, projection: str) -> str:
        """Build SELECT clause preserving original query modifiers."""
        if self.query_modifiers.get('distinct'):
            return f"SELECT DISTINCT {projection}"
        if self.query_modifiers.get('reduced'):
            return f"SELECT REDUCED {projection}"
        return f"SELECT {projection}"

    def _extract_uri_authority(self, term: str) -> Optional[str]:
        """Extract authority (scheme + host) from an IRI term."""
        if not isinstance(term, str) or term.startswith('?'):
            return None
        if term.startswith('<') and term.endswith('>'):
            term = term[1:-1]

        parsed = urlparse(term)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return None

    def _candidate_matches_authority(self, triple, candidate: Dict) -> bool:
        """Check if candidate supports bound URI authorities in triple pattern."""
        s, _, o = triple
        authorities = candidate.get('authorities') or {}

        subject_required = self._extract_uri_authority(s)
        if subject_required:
            subject_supported = set(authorities.get('subject', []))
            if subject_supported and subject_required not in subject_supported:
                return False

        object_required = self._extract_uri_authority(o)
        if object_required:
            object_supported = set(authorities.get('object', []))
            if object_supported and object_required not in object_supported:
                return False

        return True

    def _candidate_matches_variable_authority(self, triple, candidate: Dict,
                                              var_authority_state: Dict[str, Set[str]]) -> bool:
        """Check candidate against inferred/propagated variable authorities."""
        s, _, o = triple
        authorities = candidate.get('authorities') or {}

        if isinstance(s, str) and s.startswith('?'):
            required_subject = set(var_authority_state.get(s, set()))
            if required_subject:
                supported_subject = set(authorities.get('subject', []))
                if supported_subject and required_subject.isdisjoint(supported_subject):
                    return False

        if isinstance(o, str) and o.startswith('?'):
            required_object = set(var_authority_state.get(o, set()))
            if required_object:
                supported_object = set(authorities.get('object', []))
                if supported_object and required_object.isdisjoint(supported_object):
                    return False

        return True

    def _candidate_supports_triple_authorities(self, triple, candidate: Dict,
                                               var_authority_state: Dict[str, Set[str]]) -> bool:
        """Validate both bound URI and variable authority constraints for a triple."""
        return (
            self._candidate_matches_authority(triple, candidate)
            and self._candidate_matches_variable_authority(triple, candidate, var_authority_state)
        )
    
    def _build_constraint_map(self) -> Dict:
        """Build a map of variable constraints from triple patterns and metadata"""
        constraints = defaultdict(set)
        
        # Analyze each triple for class constraints
        for s, p, o in self.triple_patterns:
            # Get metadata for predicate
            pred_metadata = self.metadata.get(p, [])
            
            for meta in pred_metadata:
                subject_class = meta.get('subject_class')
                object_class = meta.get('object_class')
                
                # If subject is a variable, constrain it
                if s.startswith('?') and subject_class:
                    constraints[s].add(subject_class)
                
                # If object is a variable and it's a class, constrain it
                if o.startswith('?') and object_class:
                    constraints[o].add(object_class)
        
        return constraints

    def _infer_variable_classes(self) -> Dict[str, List[Tuple[str, float]]]:
        """Infer possible classes per variable with confidence scores.
        
        Returns:
            Dict mapping variable -> List[(class, confidence_score)]
            When deterministic (confidence=1.0), only one class.
            When uncertain, multiple classes sorted by score.
        """
        inferred = defaultdict(list)

        # 1) Deterministic constraints from predicates that map to one metadata row
        for s, p, o in self.triple_patterns:
            candidates = self.metadata.get(p, [])
            if len(candidates) != 1:
                continue

            only = candidates[0]
            if s.startswith('?') and only.get('subject_class'):
                if s not in inferred:
                    inferred[s] = [(only['subject_class'], 1.0)]  # Deterministic
            if o.startswith('?') and only.get('object_class'):
                if o not in inferred:
                    inferred[o] = [(only['object_class'], 1.0)]  # Deterministic

        # 2) Weighted fallback from all candidates (favor lower triple count)
        weighted_scores = defaultdict(lambda: defaultdict(float))
        for s, p, o in self.triple_patterns:
            for c in self.metadata.get(p, []):
                tc = c.get('triple_count') or 1
                weight = 1.0 / math.log10(tc + 10)

                subj_class = c.get('subject_class')
                obj_class = c.get('object_class')
                if s.startswith('?') and subj_class:
                    weighted_scores[s][subj_class] += weight
                if o.startswith('?') and obj_class:
                    weighted_scores[o][obj_class] += weight

        # 3) For variables without deterministic constraints, keep top candidates
        for var, class_scores in weighted_scores.items():
            if inferred[var]:  # Already has deterministic constraint
                continue
            if not class_scores:
                continue
            
            # Sort by score descending
            sorted_classes = sorted(class_scores.items(), key=lambda x: x[1], reverse=True)
            
            # Normalize scores to get confidence
            total_score = sum(score for _, score in sorted_classes)
            
            # Keep top class if it's dominant (>70% of total score)
            # Otherwise keep multiple possibilities
            top_score = sorted_classes[0][1]
            if top_score / total_score > 0.7:
                # Confident inference
                inferred[var] = [(sorted_classes[0][0], top_score / total_score)]
            else:
                # Uncertain - keep all classes with >15% of top score
                threshold = top_score * 0.15
                candidates = [(cls, score/total_score) for cls, score in sorted_classes 
                             if score >= threshold]
                inferred[var] = candidates[:5]  # Keep top 5 max

        return dict(inferred)

    def _infer_variable_authorities(self) -> Dict[str, Set[str]]:
        """Infer possible authorities per variable from predicate role metadata.

        Uses deterministic constraints when only one metadata row exists for a predicate,
        then intersects with weighted fallback candidates from all metadata rows.
        """
        deterministic = defaultdict(set)
        weighted_scores = defaultdict(lambda: defaultdict(float))

        for s, p, o in self.triple_patterns:
            candidates = self.metadata.get(p, [])

            # Deterministic authority constraints when all metadata rows agree
            # on subject/object authority sets (common for duplicated rows).
            if candidates:
                subject_signatures = {
                    tuple(sorted((c.get('authorities') or {}).get('subject', []) or []))
                    for c in candidates
                }
                object_signatures = {
                    tuple(sorted((c.get('authorities') or {}).get('object', []) or []))
                    for c in candidates
                }

                if isinstance(s, str) and s.startswith('?') and len(subject_signatures) == 1:
                    only_subject = next(iter(subject_signatures))
                    if only_subject:
                        deterministic[s].update(only_subject)

                if isinstance(o, str) and o.startswith('?') and len(object_signatures) == 1:
                    only_object = next(iter(object_signatures))
                    if only_object:
                        deterministic[o].update(only_object)

            for c in candidates:
                tc = c.get('triple_count') or 1
                weight = 1.0 / math.log10(tc + 10)
                auth = c.get('authorities') or {}

                if isinstance(s, str) and s.startswith('?'):
                    for authority in (auth.get('subject', []) or []):
                        weighted_scores[s][authority] += weight

                if isinstance(o, str) and o.startswith('?'):
                    for authority in (auth.get('object', []) or []):
                        weighted_scores[o][authority] += weight

        inferred = {}
        for var, scores in weighted_scores.items():
            if not scores:
                if deterministic[var]:
                    inferred[var] = set(deterministic[var])
                continue

            sorted_authorities = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            top_score = sorted_authorities[0][1]
            threshold = top_score * 0.2
            selected = {authority for authority, score in sorted_authorities if score >= threshold}

            if deterministic[var]:
                intersection = set(deterministic[var]).intersection(selected)
                inferred[var] = intersection if intersection else set(deterministic[var])
            else:
                inferred[var] = selected

        for var, authorities in deterministic.items():
            if var not in inferred and authorities:
                inferred[var] = set(authorities)

        return {var: vals for var, vals in inferred.items() if vals}

    def _initial_var_authority_state(self, authority_overrides: Optional[Dict[str, Set[str]]] = None) -> Dict[str, Set[str]]:
        """Create mutable variable authority constraints for assignment/scenario flow."""
        state = {
            var: set(authorities)
            for var, authorities in self.inferred_var_authority.items()
        }
        if authority_overrides:
            for var, values in authority_overrides.items():
                if not values:
                    continue
                override_set = set(values)
                if state.get(var):
                    intersection = state[var].intersection(override_set)
                    state[var] = intersection if intersection else override_set
                else:
                    state[var] = override_set
        return state

    def _narrow_var_authority(self, var_authority_state: Dict[str, Set[str]],
                              term: str, candidate_role_authorities: List[str]):
        """Intersect variable authority domain with selected candidate role authorities."""
        if not isinstance(term, str) or not term.startswith('?'):
            return

        candidate_set = set(candidate_role_authorities or [])
        if not candidate_set:
            return

        current = var_authority_state.get(term)
        if current:
            intersection = current.intersection(candidate_set)
            if intersection:
                var_authority_state[term] = intersection
        else:
            var_authority_state[term] = set(candidate_set)

    def _choose_best_candidate(self, triple, var_endpoint_affinity: Dict[str, Set[str]],
                               var_authority_state: Dict[str, Set[str]]) -> Optional[Dict]:
        """Choose best endpoint metadata candidate for one triple pattern."""
        s, p, o = triple
        candidates = self.metadata.get(p, [])
        if not candidates:
            return None

        candidates = [
            c for c in candidates
            if self._candidate_supports_triple_authorities(triple, c, var_authority_state)
        ]
        if not candidates:
            return None

        def score(c):
            value = 0.0

            subj_class = c.get('subject_class')
            obj_class = c.get('object_class')
            endpoint = c.get('endpoint')
            tc = c.get('triple_count') or 1

            # Check if subject class matches any of the inferred possibilities
            if s.startswith('?') and s in self.inferred_var_class:
                for inferred_class, confidence in self.inferred_var_class[s]:
                    if subj_class == inferred_class:
                        value += 12 * confidence  # Weighted by confidence
                        break
            
            # Check if object class matches any of the inferred possibilities
            if o.startswith('?') and o in self.inferred_var_class:
                for inferred_class, confidence in self.inferred_var_class[o]:
                    if obj_class == inferred_class:
                        value += 12 * confidence  # Weighted by confidence
                        break

            # Prefer keeping joins on already-used endpoint for shared variables
            if s.startswith('?') and endpoint in var_endpoint_affinity.get(s, set()):
                value += 5
            if o.startswith('?') and endpoint in var_endpoint_affinity.get(o, set()):
                value += 5

            # Prefer selective partitions
            value -= math.log10(tc + 1)
            return value

        return max(candidates, key=score)

    def _candidate_count_or_inf(self, candidate: Dict) -> float:
        """Return candidate triple_count as numeric value or infinity."""
        count = candidate.get('triple_count')
        if isinstance(count, (int, float)):
            return float(count)
        return float('inf')

    def _assign_endpoints(self, authority_overrides: Optional[Dict[str, Set[str]]] = None) -> List[Dict]:
        """Assign each triple to best endpoint candidate using class constraints and join affinity."""
        assigned = []
        var_endpoint_affinity = defaultdict(set)
        var_authority_state = self._initial_var_authority_state(authority_overrides)

        # Process selective triples first
        sorted_triples = sorted(
            self.triple_patterns,
            key=lambda t: min(
                [self._candidate_count_or_inf(c) for c in self.metadata.get(t[1], [])] or [float('inf')]
            )
        )

        for triple in sorted_triples:
            s, p, o = triple
            candidate = self._choose_best_candidate(triple, var_endpoint_affinity, var_authority_state)

            if not candidate:
                endpoint_votes = defaultdict(int)
                if s.startswith('?'):
                    for ep in var_endpoint_affinity.get(s, set()):
                        endpoint_votes[ep] += 2
                if o.startswith('?'):
                    for ep in var_endpoint_affinity.get(o, set()):
                        endpoint_votes[ep] += 1

                inferred_endpoint = max(endpoint_votes.items(), key=lambda x: x[1])[0] if endpoint_votes else 'unknown'
                assigned.append({
                    'triple': triple,
                    'endpoint': inferred_endpoint,
                    'subject_class': None,
                    'object_class': None,
                    'triple_count': None
                })

                if inferred_endpoint != 'unknown':
                    if s.startswith('?'):
                        var_endpoint_affinity[s].add(inferred_endpoint)
                    if o.startswith('?'):
                        var_endpoint_affinity[o].add(inferred_endpoint)
                continue

            endpoint = candidate['endpoint']
            if s.startswith('?'):
                var_endpoint_affinity[s].add(endpoint)
            if o.startswith('?'):
                var_endpoint_affinity[o].add(endpoint)

            candidate_authorities = candidate.get('authorities') or {}
            self._narrow_var_authority(var_authority_state, s, candidate_authorities.get('subject', []))
            self._narrow_var_authority(var_authority_state, o, candidate_authorities.get('object', []))

            assigned.append({
                'triple': triple,
                'endpoint': endpoint,
                'subject_class': candidate.get('subject_class'),
                'object_class': candidate.get('object_class'),
                'triple_count': candidate.get('triple_count')
            })

        return assigned
    
    def generate_smart_query(self) -> str:
        """Generate query with intelligent class-based constraints and SERVICE blocks.
        
        If variables have multiple possible class interpretations that lead to
        different endpoints, generate UNION query to cover all cases.
        """
        # Check if we need UNION (any variable with multiple class possibilities)
        has_class_ambiguity = any(len(classes) > 1 for classes in self.inferred_var_class.values())
        has_authority_ambiguity = any(len(authorities) > 1 for authorities in self.inferred_var_authority.values())
        needs_union = has_class_ambiguity or has_authority_ambiguity
        
        if needs_union:
            # Check if different interpretations actually lead to different queries
            return self._generate_union_query_if_needed()
        else:
            return self._generate_simple_query()
    
    def _generate_simple_query(self) -> str:
        """Generate simple query when all variables have single class interpretation."""
        query = []
        query.append("# Smart Federated Query with Class Constraints\n")
        query.append("# Variables are constrained by inferred class types and grouped by endpoint\n\n")
        
        projection_vars = self._get_projection_variables()
        projection = " ".join(projection_vars) if projection_vars else "*"

        query.append(f"{self._build_select_clause(projection)}\n")
        query.append("WHERE {\n")

        assigned = self._assign_endpoints()
        endpoint_groups = defaultdict(list)
        for row in assigned:
            endpoint_groups[row['endpoint']].append(row)

        for endpoint in sorted(endpoint_groups.keys()):
            group = endpoint_groups[endpoint]
            if endpoint != 'unknown':
                query.append(f"\n  SERVICE <{endpoint}> {{\n")
            else:
                query.append("\n  # Unknown endpoint (no metadata)\n")

            optional_rows = []
            for row in group:
                s, p, o = row['triple']
                is_optional_mw = 'molecularWeightAverage' in p

                class_note_parts = []

                if s.startswith('?') and row.get('subject_class'):
                    class_note_parts.append(f"{s}:{self._shorten_uri(row['subject_class'])}")
                if o.startswith('?') and row.get('object_class'):
                    class_note_parts.append(f"{o}:{self._shorten_uri(row['object_class'])}")

                class_note = f" # {', '.join(class_note_parts)}" if class_note_parts else ""
                triple_line = f"    {s} {p} {o} .{class_note}\n"

                if is_optional_mw:
                    optional_rows.append(triple_line)
                else:
                    query.append(triple_line)

            if optional_rows:
                query.append("    OPTIONAL {\n")
                for line in optional_rows:
                    query.append(line)
                query.append("      FILTER (?molecularWeightAverage > 114)\n")
                query.append("    }\n")

            if endpoint != 'unknown':
                query.append("  }\n")

        query.append("}\n")
        query.append("LIMIT 1000\n")
        
        return "".join(query)
    
    def _generate_union_query_if_needed(self) -> str:
        """Generate UNION query only if different scenarios produce different endpoint assignments."""
        # Generate all class and authority interpretation scenarios
        scenarios = self._generate_combined_scenarios()
        
        # Get endpoint assignment signature for each scenario
        scenario_signatures = []
        for scenario in scenarios:
            signature = self._get_scenario_signature(scenario)
            scenario_signatures.append((scenario, signature))
        
        # Filter to unique signatures only
        unique_scenarios = []
        seen_signatures = set()
        for scenario, signature in scenario_signatures:
            if signature not in seen_signatures:
                unique_scenarios.append(scenario)
                seen_signatures.add(signature)
        
        # If all scenarios are the same, just generate a simple query
        if len(unique_scenarios) <= 1:
            return self._generate_simple_query_with_note(scenarios[0] if scenarios else {})
        
        # Generate UNION query for distinct scenarios
        return self._generate_union_query_for_multiple_cases(unique_scenarios)
    
    def _get_scenario_signature(self, scenario: Dict[str, Dict]) -> str:
        """Get a signature representing the endpoint assignment for a scenario.
        
        Returns a string that uniquely identifies which endpoints and predicates
        are used for this scenario.
        """
        # Temporarily set single-class interpretation
        original_inferred = self.inferred_var_class
        class_scenario = scenario.get('classes', {})
        authority_scenario = scenario.get('authorities', {})
        self.inferred_var_class = {
            var: [(cls, 1.0)] for var, cls in class_scenario.items()
        }
        
        try:
            # Assign endpoints for this scenario
            assigned = self._assign_endpoints(authority_overrides=authority_scenario)
            
            # Create signature: sorted list of (endpoint, subject, predicate, object)
            signature_parts = []
            for row in assigned:
                s, p, o = row['triple']
                endpoint = row['endpoint']
                signature_parts.append(f"{endpoint}||{s}||{p}||{o}")
            
            signature_parts.sort()
            return "###".join(signature_parts)
        finally:
            # Restore original inferred classes
            self.inferred_var_class = original_inferred
    
    def _generate_simple_query_with_note(self, scenario: Dict[str, Dict]) -> str:
        """Generate simple query with note about class ambiguity being irrelevant."""
        query = []
        query.append("# Smart Federated Query with Class Constraints\n")
        query.append("# Note: Multiple class interpretations exist but all route to same endpoints\n")
        query.append("# A single query retrieves all data regardless of class ambiguity\n\n")
        
        projection_vars = self._get_projection_variables()
        projection = " ".join(projection_vars) if projection_vars else "*"

        query.append(f"{self._build_select_clause(projection)}\n")
        query.append("WHERE {\n")

        assigned = self._assign_endpoints()
        endpoint_groups = defaultdict(list)
        for row in assigned:
            endpoint_groups[row['endpoint']].append(row)

        for endpoint in sorted(endpoint_groups.keys()):
            group = endpoint_groups[endpoint]
            if endpoint != 'unknown':
                query.append(f"\n  SERVICE <{endpoint}> {{\n")
            else:
                query.append("\n  # Unknown endpoint (no metadata)\n")

            optional_rows = []
            for row in group:
                s, p, o = row['triple']
                is_optional_mw = 'molecularWeightAverage' in p

                class_note_parts = []

                if s.startswith('?') and row.get('subject_class'):
                    class_note_parts.append(f"{s}:{self._shorten_uri(row['subject_class'])}")
                if o.startswith('?') and row.get('object_class'):
                    class_note_parts.append(f"{o}:{self._shorten_uri(row['object_class'])}")

                class_note = f" # {', '.join(class_note_parts)}" if class_note_parts else ""
                triple_line = f"    {s} {p} {o} .{class_note}\n"

                if is_optional_mw:
                    optional_rows.append(triple_line)
                else:
                    query.append(triple_line)

            if optional_rows:
                query.append("    OPTIONAL {\n")
                for line in optional_rows:
                    query.append(line)
                query.append("      FILTER (?molecularWeightAverage > 114)\n")
                query.append("    }\n")

            if endpoint != 'unknown':
                query.append("  }\n")

        query.append("}\n")
        query.append("LIMIT 1000\n")
        
        return "".join(query)
    
    def _generate_union_query_for_multiple_cases(self, scenarios: List[Dict[str, Dict]]) -> str:
        """Generate UNION query covering distinct class interpretation scenarios."""
        query = []
        query.append("# Smart Federated Query with UNION for Multiple Class Interpretations\n")
        query.append("# Each UNION branch uses different endpoints or predicates\n\n")
        
        projection_vars = self._get_projection_variables()
        projection = " ".join(projection_vars) if projection_vars else "*"

        query.append(f"{self._build_select_clause(projection)}\n")
        query.append("WHERE {\n")
        
        for i, scenario in enumerate(scenarios):
            if i > 0:
                query.append("\n  UNION\n\n")
            
            query.append(f"  # Scenario {i+1}: ")
            class_desc = [
                f"{var}={self._shorten_uri(cls)}"
                for var, cls in scenario.get('classes', {}).items()
            ]
            auth_desc = [
                f"{var}={auth.split('://', 1)[-1]}"
                for var, auth_set in scenario.get('authorities', {}).items()
                for auth in sorted(auth_set)
            ]
            scenario_desc = ", ".join(class_desc + auth_desc)
            query.append(f"{scenario_desc}\n")
            query.append("  {\n")
            
            # Generate query for this specific scenario
            scenario_query = self._generate_query_for_scenario(scenario)
            query.append(scenario_query)
            
            query.append("  }\n")
        
        query.append("}\n")
        query.append("LIMIT 1000\n")
        
        return "".join(query)
    
    def _generate_class_scenarios(self) -> List[Dict[str, str]]:
        """Generate all possible class interpretation scenarios.
        
        Returns list of dictionaries mapping variable -> class for each scenario.
        Limits to reasonable number of combinations.
        """
        # Get variables with multiple possibilities
        multi_class_vars = {
            var: classes for var, classes in self.inferred_var_class.items()
            if len(classes) > 1
        }
        
        # Get variables with single class (fixed)
        single_class_vars = {
            var: classes[0][0] for var, classes in self.inferred_var_class.items()
            if len(classes) == 1
        }
        
        if not multi_class_vars:
            return [single_class_vars] if single_class_vars else [{}]
        
        # Generate combinations, but limit to avoid explosion
        # For each ambiguous variable, take top 2 classes max
        import itertools
        
        var_options = {}
        for var, classes in multi_class_vars.items():
            # Take top 2 classes or all if confidence difference is small
            top_conf = classes[0][1]
            candidates = [(cls, conf) for cls, conf in classes[:3] 
                         if conf >= top_conf * 0.3]  # Within 30% of top
            var_options[var] = [cls for cls, conf in candidates[:2]]  # Max 2
        
        # Generate cartesian product of options
        vars_list = list(var_options.keys())
        options_list = [var_options[v] for v in vars_list]
        
        scenarios = []
        for combination in itertools.product(*options_list):
            scenario = dict(single_class_vars)  # Start with fixed classes
            scenario.update(dict(zip(vars_list, combination)))
            scenarios.append(scenario)
        
        # Limit to 4 scenarios max to keep query manageable
        scenarios = scenarios[:4]
        return scenarios

    def _generate_authority_scenarios(self) -> List[Dict[str, Set[str]]]:
        """Generate authority interpretation scenarios for variables with multiple candidates."""
        multi_authority_vars = {
            var: sorted(list(authorities))
            for var, authorities in self.inferred_var_authority.items()
            if len(authorities) > 1
        }
        single_authority_vars = {
            var: set(authorities)
            for var, authorities in self.inferred_var_authority.items()
            if len(authorities) == 1
        }

        if not multi_authority_vars:
            return [single_authority_vars] if single_authority_vars else [{}]

        import itertools

        var_options = {}
        for var, authorities in multi_authority_vars.items():
            var_options[var] = authorities[:2]

        vars_list = list(var_options.keys())
        options_list = [var_options[v] for v in vars_list]

        scenarios = []
        for combination in itertools.product(*options_list):
            scenario = dict(single_authority_vars)
            scenario.update({var: {auth} for var, auth in zip(vars_list, combination)})
            scenarios.append(scenario)

        return scenarios[:4]

    def _generate_combined_scenarios(self) -> List[Dict[str, Dict]]:
        """Generate combined class+authority scenarios with authority feasibility checks."""
        class_scenarios = self._generate_class_scenarios()
        authority_scenarios = self._generate_authority_scenarios()

        combined = []
        for class_scenario in class_scenarios:
            for authority_scenario in authority_scenarios:
                scenario = {
                    'classes': class_scenario,
                    'authorities': authority_scenario
                }
                if self._scenario_matches_authority(scenario):
                    combined.append(scenario)
                if len(combined) >= 8:
                    return combined

        return combined or [{'classes': {}, 'authorities': {}}]

    def _scenario_matches_authority(self, scenario: Dict[str, Dict]) -> bool:
        """Validate that scenario can satisfy authority constraints."""
        class_scenario = scenario.get('classes', {})
        authority_scenario = scenario.get('authorities', {})
        original_inferred = self.inferred_var_class
        self.inferred_var_class = {
            var: [(cls, 1.0)] for var, cls in class_scenario.items()
        }
        var_authority_state = self._initial_var_authority_state(authority_scenario)

        try:
            for triple in self.triple_patterns:
                s, _, o = triple
                predicate = triple[1]
                candidates = self.metadata.get(predicate, [])
                if not candidates:
                    continue
                valid_candidates = [
                    c for c in candidates
                    if self._candidate_supports_triple_authorities(triple, c, var_authority_state)
                ]
                if not valid_candidates:
                    return False

                # Propagate narrowed authorities with strongest (lowest triple_count) candidate
                best = min(valid_candidates, key=self._candidate_count_or_inf)
                best_auth = best.get('authorities') or {}
                self._narrow_var_authority(var_authority_state, s, best_auth.get('subject', []))
                self._narrow_var_authority(var_authority_state, o, best_auth.get('object', []))
            return True
        finally:
            self.inferred_var_class = original_inferred
    
    def _generate_query_for_scenario(self, scenario: Dict[str, Dict]) -> str:
        """Generate query fragment for a specific class scenario."""
        class_scenario = scenario.get('classes', {})
        authority_scenario = scenario.get('authorities', {})
        # Temporarily set single-class interpretation
        original_inferred = self.inferred_var_class
        self.inferred_var_class = {
            var: [(cls, 1.0)] for var, cls in class_scenario.items()
        }
        
        try:
            # Assign endpoints for this scenario
            assigned = self._assign_endpoints(authority_overrides=authority_scenario)
            endpoint_groups = defaultdict(list)
            for row in assigned:
                endpoint_groups[row['endpoint']].append(row)
            
            query = []
            for endpoint in sorted(endpoint_groups.keys()):
                group = endpoint_groups[endpoint]
                if endpoint != 'unknown':
                    query.append(f"    SERVICE <{endpoint}> {{\n")
                else:
                    query.append("    # Unknown endpoint (no metadata)\n")

                optional_rows = []
                for row in group:
                    s, p, o = row['triple']
                    is_optional_mw = 'molecularWeightAverage' in p

                    class_note_parts = []
                    if s.startswith('?') and row.get('subject_class'):
                        class_note_parts.append(f"{s}:{self._shorten_uri(row['subject_class'])}")
                    if o.startswith('?') and row.get('object_class'):
                        class_note_parts.append(f"{o}:{self._shorten_uri(row['object_class'])}")

                    class_note = f" # {', '.join(class_note_parts)}" if class_note_parts else ""
                    triple_line = f"      {s} {p} {o} .{class_note}\n"

                    if is_optional_mw:
                        optional_rows.append(triple_line)
                    else:
                        query.append(triple_line)

                if optional_rows:
                    query.append("      OPTIONAL {\n")
                    for line in optional_rows:
                        query.append(line)
                    query.append("        FILTER (?molecularWeightAverage > 114)\n")
                    query.append("      }\n")

                if endpoint != 'unknown':
                    query.append("    }\n")
            
            return "".join(query)
        finally:
            # Restore original inferred classes
            self.inferred_var_class = original_inferred
    
    def _shorten_uri(self, uri):
        """Shorten URI"""
        if uri.startswith('<') and uri.endswith('>'):
            uri = uri[1:-1]
        if '#' in uri:
            return uri.split('#')[-1]
        return uri.split('/')[-1] if '/' in uri else uri


def main():
    """Run optimizer for a single analysis file or all analysis files in a directory"""
    parser = argparse.ArgumentParser(description="Generate federated query optimizations from analysis JSON")
    parser.add_argument(
        '--analysis-file',
        help='Path to a single analysis JSON file'
    )
    parser.add_argument(
        '--analysis-dir',
        default='./optimization',
        help='Directory containing per-query *.analysis.json files'
    )
    parser.add_argument(
        '--output-dir',
        default='./optimization',
        help='Output directory for optimization artifacts'
    )
    parser.add_argument(
        '--legacy-output',
        action='store_true',
        help='Also write legacy root output files for compatibility'
    )
    parser.add_argument(
        '--no-print-query',
        action='store_true',
        help='Do not print generated optimized query text to console'
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.analysis_file:
        analysis_files = [Path(args.analysis_file)]
    else:
        analysis_dir = Path(args.analysis_dir)
        if not analysis_dir.exists() or not analysis_dir.is_dir():
            print(f"Error: analysis directory not found: {analysis_dir}")
            raise SystemExit(1)
        analysis_files = sorted(analysis_dir.glob('*.analysis.json'))

    if not analysis_files:
        print("Error: no analysis files found")
        raise SystemExit(1)

    run_summary = {
        'analysis_inputs': [str(p) for p in analysis_files],
        'output_dir': str(output_dir),
        'total_queries': len(analysis_files),
        'processed': 0,
        'failed': 0,
        'partial_optimizations': 0,
        'queries': []
    }

    print("=" * 80)
    print(f"Running optimization for {len(analysis_files)} query analyses...")
    print("=" * 80)

    for analysis_path in analysis_files:
        query_id = analysis_path.name.replace('.analysis.json', '')
        status_entry = {
            'query_id': query_id,
            'analysis_file': str(analysis_path),
            'status': 'failed'
        }

        try:
            with open(analysis_path, 'r') as f:
                analysis = json.load(f)

            optimizer = FederatedQueryOptimizer()
            optimizer.load_query_analysis(analysis)
            result = optimizer.generate_federated_query(
                "SELECT ...",
                use_service_clauses=True
            )

            smart_builder = SmartFederatedQueryBuilder(analysis)
            smart_query = smart_builder.generate_smart_query()
            report = optimizer.generate_optimization_report()

            serializable_constraints = {
                var: sorted(list(classes))
                for var, classes in dict(smart_builder.constraint_map).items()
            }
            serializable_inferred = {
                var: [{'class': cls, 'confidence': conf} for cls, conf in classes]
                for var, classes in smart_builder.inferred_var_class.items()
            }
            serializable_inferred_authorities = {
                var: sorted(list(authorities))
                for var, authorities in smart_builder.inferred_var_authority.items()
            }

            assigned_triples = smart_builder._assign_endpoints()
            unknown_assignments = [
                row['triple'] for row in assigned_triples
                if row.get('endpoint') == 'unknown'
            ]

            metadata_coverage = analysis.get('metadata_coverage', {})
            is_partial = bool(metadata_coverage.get('is_partial', False) or unknown_assignments)

            optimization_payload = {
                'query_id': query_id,
                'analysis_file': str(analysis_path),
                'optimization_status': 'partial' if is_partial else 'complete',
                'metadata_coverage': metadata_coverage,
                'unknown_endpoint_triples': unknown_assignments,
                'optimization_analysis': result,
                'constraint_map': serializable_constraints,
                'inferred_variable_classes': serializable_inferred,
                'inferred_variable_authorities': serializable_inferred_authorities,
                'optimized_federated_query': smart_query,
                'predicate_selectivity': optimizer.selective_predicates
            }

            optimization_file = output_dir / f"{query_id}.optimization.json"
            with open(optimization_file, 'w') as f:
                json.dump(optimization_payload, f, indent=2)

            report_file = output_dir / f"{query_id}.report.txt"
            report_with_status = report
            report_with_status += "\n\n" + ("-" * 80)
            report_with_status += "\nOPTIMIZATION STATUS"
            report_with_status += "\n" + ("-" * 80)
            report_with_status += f"\nStatus: {'partial' if is_partial else 'complete'}"
            if metadata_coverage:
                report_with_status += (
                    f"\nMetadata Coverage: {metadata_coverage.get('covered_predicates', 0)}"
                    f"/{metadata_coverage.get('total_predicates', 0)}"
                )
            if unknown_assignments:
                report_with_status += f"\nUnknown Endpoint Triples: {len(unknown_assignments)}"
            report_with_status += "\n\n" + ("-" * 80)
            report_with_status += "\nGENERATED FEDERATED QUERY"
            report_with_status += "\n" + ("-" * 80)
            report_with_status += "\n" + smart_query
            with open(report_file, 'w') as f:
                f.write(report_with_status)

            status_entry['status'] = 'ok'
            status_entry['optimization_file'] = str(optimization_file)
            status_entry['report_file'] = str(report_file)
            status_entry['optimization_status'] = optimization_payload['optimization_status']
            status_entry['unknown_endpoint_triples'] = len(unknown_assignments)
            run_summary['processed'] += 1
            if is_partial:
                run_summary['partial_optimizations'] += 1

            print(f"✓ {query_id} -> {optimization_file.name}, {report_file.name}")

            if not args.no_print_query:
                print("-" * 80)
                print(f"Optimized query for {query_id}:")
                print("-" * 80)
                print(smart_query)
                print("-" * 80)

            if args.legacy_output and args.analysis_file:
                with open('optimization_report.txt', 'w') as f:
                    f.write(report_with_status)
                with open('federated_query_optimization.json', 'w') as f:
                    json.dump(optimization_payload, f, indent=2)
                print("✓ Legacy outputs written: optimization_report.txt, federated_query_optimization.json")

        except Exception as e:
            status_entry['error'] = str(e)
            run_summary['failed'] += 1
            print(f"✗ {query_id} -> {e}")

        run_summary['queries'].append(status_entry)

    summary_file = output_dir / 'optimizer_run_summary.json'
    with open(summary_file, 'w') as f:
        json.dump(run_summary, f, indent=2)

    print("=" * 80)
    print("Optimization complete")
    print(f"Processed: {run_summary['processed']}/{run_summary['total_queries']}")
    print(f"Failed: {run_summary['failed']}")
    print(f"Partial optimizations: {run_summary['partial_optimizations']}")
    print(f"Run summary: {summary_file}")
    print("=" * 80)


if __name__ == '__main__':
    main()
