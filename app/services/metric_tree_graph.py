from __future__ import annotations

import ast
from collections import defaultdict, deque

from app.api.schemas.config import MetricTreeGraphPayload

_CANONICAL_INPUT_NODES = {
    "mau",
    "penetration",
    "conversion",
    "frequency",
    "aoq",
    "aiv",
    "fm_pct",
}
_CANONICAL_DERIVED_FORMULAS: dict[str, tuple[str, ...]] = {
    "mau_effective": ("mau", "penetration"),
    "orders": ("mau_effective", "conversion", "frequency"),
    "items": ("orders", "aoq"),
    "aov": ("aoq", "aiv"),
    "rto": ("orders", "aov"),
    "fm": ("rto", "fm_pct"),
}
_FORBIDDEN_NODE_IDS = {"gmv", "margin"}
_ALLOWED_FUNCTIONS = {"min", "max", "abs"}
_ALLOWED_EXTERNAL_INPUTS = {"frequency_monthly"}


def validate_metric_tree_graph(graph: MetricTreeGraphPayload) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    node_ids = [node.node_id for node in graph.nodes]
    node_set = set(node_ids)
    if len(node_ids) != len(node_set):
        errors.append("Graph contains duplicate node_id values.")

    for forbidden in sorted(node_id for node_id in node_set if _is_forbidden_node(node_id)):
        errors.append(
            f"Node `{forbidden}` is deprecated for current metric trees. Use canonical `rto`/`fm` only."
        )

    edges_by_child: dict[str, set[str]] = defaultdict(set)
    indegree = defaultdict(int)
    adjacency: dict[str, list[str]] = defaultdict(list)
    for edge in graph.edges:
        if edge.from_node not in node_set:
            errors.append(f"Edge from_node `{edge.from_node}` is not defined in nodes.")
            continue
        if edge.to_node not in node_set:
            errors.append(f"Edge to_node `{edge.to_node}` is not defined in nodes.")
            continue
        if edge.from_node == edge.to_node:
            errors.append(f"Node `{edge.from_node}` cannot depend on itself.")
            continue
        edges_by_child[edge.to_node].add(edge.from_node)
        adjacency[edge.from_node].append(edge.to_node)
        indegree[edge.to_node] += 1
        indegree.setdefault(edge.from_node, indegree.get(edge.from_node, 0))

    for node_id in node_set:
        indegree.setdefault(node_id, 0)

    roots = [node_id for node_id in node_set if indegree.get(node_id, 0) == 0]
    queue = deque(sorted(roots))
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for nxt in adjacency.get(current, []):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if visited != len(node_set):
        errors.append("Metric tree graph must be acyclic (DAG validation failed).")

    nodes_by_id = {node.node_id: node for node in graph.nodes}

    missing_inputs = sorted(_CANONICAL_INPUT_NODES - node_set)
    if missing_inputs:
        errors.append(
            "Graph is missing required canonical input nodes: " + ", ".join(missing_inputs)
        )

    missing_derived = sorted(set(_CANONICAL_DERIVED_FORMULAS) - node_set)
    if missing_derived:
        errors.append(
            "Graph is missing required canonical derived nodes: " + ", ".join(missing_derived)
        )

    for node in graph.nodes:
        node_id = node.node_id
        refs = _extract_formula_refs(node.formula)
        edge_parents = edges_by_child.get(node_id, set())

        if node_id in _CANONICAL_INPUT_NODES:
            if node.formula:
                errors.append(f"Canonical input node `{node_id}` must not define a formula.")
            if edge_parents:
                warnings.append(
                    f"Input node `{node_id}` has inbound edges; these edges will be ignored by runtime evaluation."
                )
            if not node.is_targetable:
                errors.append(f"Canonical input node `{node_id}` must be marked is_targetable=true.")
            continue

        if node_id in _CANONICAL_DERIVED_FORMULAS:
            expected_refs = set(_CANONICAL_DERIVED_FORMULAS[node_id])
            if not node.formula:
                errors.append(f"Canonical derived node `{node_id}` must define a formula.")
                continue
            if set(refs) != expected_refs:
                errors.append(
                    f"Node `{node_id}` formula must reference exactly: {', '.join(sorted(expected_refs))}."
                )
            if edge_parents != expected_refs:
                errors.append(
                    f"Node `{node_id}` inbound edges must match formula refs exactly: {', '.join(sorted(expected_refs))}."
                )
            if node.is_targetable:
                errors.append(f"Canonical derived node `{node_id}` must not be targetable.")
            continue

        if not node.formula:
            errors.append(
                f"Non-canonical node `{node_id}` must define a formula. New primitive/input metrics require backend support."
            )
            continue
        if set(refs) != edge_parents:
            errors.append(
                f"Node `{node_id}` inbound edges must match formula refs exactly: {', '.join(sorted(refs)) or 'none'}."
            )
        if node.is_targetable:
            errors.append(
                f"Node `{node_id}` cannot be targetable. Targetable nodes are restricted to canonical primitive inputs."
            )

    if len(roots) != len(_CANONICAL_INPUT_NODES):
        warnings.append(
            "Graph roots should normally match the canonical primitive inputs only."
        )

    return errors, warnings


def _extract_formula_refs(formula: str | None) -> list[str]:
    if not formula:
        return []
    tree = ast.parse(formula, mode="eval")
    refs: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Name):
            continue
        if node.id in _ALLOWED_FUNCTIONS:
            continue
        if node.id in _ALLOWED_EXTERNAL_INPUTS:
            refs.add(node.id)
            continue
        refs.add(node.id)
    return sorted(refs)


def _is_forbidden_node(node_id: str) -> bool:
    return node_id in _FORBIDDEN_NODE_IDS or node_id.startswith("aoq_component:")
