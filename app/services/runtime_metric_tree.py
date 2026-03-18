from __future__ import annotations

import ast
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import reduce
from typing import Any

import numpy as np

from app.core.errors import ValidationError


@dataclass(frozen=True)
class RuntimeMetricNode:
    node_id: str
    formula: str | None = None


@dataclass(frozen=True)
class RuntimeMetricTree:
    nodes: dict[str, RuntimeMetricNode]
    order: tuple[str, ...]
    dependencies: dict[str, tuple[str, ...]]

    def evaluate(self, inputs: dict[str, Any]) -> dict[str, Any]:
        values = dict(inputs)
        for node_id in self.order:
            formula = self.nodes[node_id].formula
            if not formula:
                if node_id not in values:
                    raise ValidationError(f"Runtime metric tree is missing input node `{node_id}`")
                continue
            values[node_id] = _safe_eval_formula(formula, values)
        return values


_DEFAULT_TREE_NODES: tuple[RuntimeMetricNode, ...] = (
    RuntimeMetricNode("mau"),
    RuntimeMetricNode("penetration"),
    RuntimeMetricNode("conversion"),
    RuntimeMetricNode("frequency"),
    RuntimeMetricNode("aoq"),
    RuntimeMetricNode("aiv"),
    RuntimeMetricNode("fm_pct"),
    RuntimeMetricNode("mau_effective", "mau * penetration"),
    RuntimeMetricNode("orders", "mau_effective * conversion * frequency"),
    RuntimeMetricNode("items", "orders * aoq"),
    RuntimeMetricNode("aov", "aoq * aiv"),
    RuntimeMetricNode("rto", "orders * aov"),
    RuntimeMetricNode("fm", "rto * fm_pct"),
)

_ALLOWED_INPUTS = {
    "mau",
    "penetration",
    "conversion",
    "frequency",
    "frequency_weekly",
    "frequency_monthly",
    "aoq",
    "aiv",
    "aov",
    "fm_pct",
}
_ALLOWED_FUNCTIONS = {"min", "max", "abs"}


def build_runtime_metric_tree(definition: dict[str, Any] | list[dict[str, Any]] | None = None) -> RuntimeMetricTree:
    nodes: dict[str, RuntimeMetricNode] = {node.node_id: node for node in _DEFAULT_TREE_NODES}

    for node in _iter_definition_nodes(definition):
        existing = nodes.get(node.node_id)
        if node.formula or existing is None:
            nodes[node.node_id] = node

    dependencies: dict[str, tuple[str, ...]] = {}
    for node_id, node in nodes.items():
        deps = tuple(_extract_dependencies(node.formula, nodes.keys()))
        dependencies[node_id] = deps

    indegree = defaultdict(int)
    adjacency: dict[str, list[str]] = defaultdict(list)
    for node_id, deps in dependencies.items():
        indegree.setdefault(node_id, 0)
        for dep in deps:
            adjacency[dep].append(node_id)
            indegree[node_id] += 1

    queue = deque(sorted(node_id for node_id, degree in indegree.items() if degree == 0))
    order: list[str] = []
    while queue:
        current = queue.popleft()
        order.append(current)
        for nxt in adjacency.get(current, []):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    if len(order) != len(nodes):
        raise ValidationError("Runtime metric tree contains cyclic or unresolved dependencies")

    required = {
        "mau",
        "penetration",
        "conversion",
        "frequency",
        "aoq",
        "aiv",
        "fm_pct",
        "mau_effective",
        "orders",
        "items",
        "aov",
        "rto",
        "fm",
    }
    missing = required - set(nodes)
    if missing:
        raise ValidationError("Runtime metric tree is missing required nodes: " + ", ".join(sorted(missing)))

    return RuntimeMetricTree(nodes=nodes, order=tuple(order), dependencies=dependencies)


def _iter_definition_nodes(definition: dict[str, Any] | list[dict[str, Any]] | None) -> list[RuntimeMetricNode]:
    if not definition:
        return []

    rows: list[dict[str, Any]]
    if isinstance(definition, dict) and isinstance(definition.get("nodes"), list):
        rows = [row for row in definition["nodes"] if isinstance(row, dict)]
        return [
            RuntimeMetricNode(
                node_id=str(row["node_id"]),
                formula=str(row["formula"]) if row.get("formula") else None,
            )
            for row in rows
            if row.get("node_id")
        ]

    if isinstance(definition, list):
        rows = [row for row in definition if isinstance(row, dict)]
        return [
            RuntimeMetricNode(
                node_id=str(row["node_id"]),
                formula=str(row["metric_formula"]) if row.get("metric_formula") else None,
            )
            for row in rows
            if row.get("node_id")
        ]

    return []


def _extract_dependencies(formula: str | None, node_ids: Any) -> list[str]:
    if not formula:
        return []
    tree = ast.parse(formula, mode="eval")
    known_nodes = set(node_ids)
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    invalid = [name for name in names if name not in known_nodes and name not in _ALLOWED_INPUTS and name not in _ALLOWED_FUNCTIONS]
    if invalid:
        raise ValidationError(
            "Runtime metric tree formula references unsupported names: " + ", ".join(sorted(invalid))
        )
    return sorted(name for name in names if name in known_nodes)


def _safe_eval_formula(formula: str, values: dict[str, Any]) -> Any:
    tree = ast.parse(formula, mode="eval")
    return _eval_node(tree.body, values)


def _eval_node(node: ast.AST, values: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in values:
            return values[node.id]
        raise ValidationError(f"Runtime metric tree references unresolved name `{node.id}`")
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, values)
        right = _eval_node(node.right, values)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.Pow):
            return left**right
        raise ValidationError(f"Unsupported runtime metric tree operator `{type(node.op).__name__}`")
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, values)
        if isinstance(node.op, ast.UAdd):
            return operand
        if isinstance(node.op, ast.USub):
            return -operand
        raise ValidationError(f"Unsupported runtime metric tree unary operator `{type(node.op).__name__}`")
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ValidationError("Unsupported runtime metric tree callable expression")
        func_name = node.func.id
        args = [_eval_node(arg, values) for arg in node.args]
        return _call_allowed_function(func_name, args)
    raise ValidationError(f"Unsupported runtime metric tree AST node `{type(node).__name__}`")


def _call_allowed_function(name: str, args: list[Any]) -> Any:
    if name not in _ALLOWED_FUNCTIONS:
        raise ValidationError(f"Unsupported runtime metric tree function `{name}`")
    if not args:
        raise ValidationError(f"Runtime metric tree function `{name}` requires arguments")
    if name == "abs":
        if len(args) != 1:
            raise ValidationError("Runtime metric tree function `abs` requires exactly one argument")
        return np.abs(args[0])
    if name == "max":
        return reduce(np.maximum, args)
    if name == "min":
        return reduce(np.minimum, args)
    raise ValidationError(f"Unsupported runtime metric tree function `{name}`")
