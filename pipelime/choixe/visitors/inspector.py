from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Set

import pydash as py_

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import load


@dataclass
class Inspection:
    imports: Set[Path] = field(default_factory=set)
    variables: Dict[str, Any] = field(default_factory=dict)
    environ: Dict[str, Any] = field(default_factory=dict)
    symbols: Set[str] = field(default_factory=set)
    processed: bool = False

    def _iteratee(self, obj_value, src_value, key, obj, source) -> Any:
        res = None
        if obj_value is not None and src_value is None:
            res = obj_value
        return res

    def __add__(self, other: Inspection) -> Inspection:
        return Inspection(
            imports={*self.imports, *other.imports},
            variables=py_.merge_with(
                self.variables, other.variables, iteratee=self._iteratee
            ),
            environ={**self.environ, **other.environ},
            symbols={*self.symbols, *other.symbols},
            processed=self.processed and other.processed,
        )


class Inspector(ast.NodeVisitor):
    def __init__(self, cwd: Optional[Path] = None) -> None:
        super().__init__()
        self._cwd = cwd if cwd is not None else Path(os.getcwd())
        self._named_for_loops: Dict[str, str] = {}

    def visit_dict(self, node: ast.DictNode) -> Inspection:
        inspections = []
        for k, v in node.nodes.items():
            inspections.append(k.accept(self))
            inspections.append(v.accept(self))
        return sum(inspections, start=Inspection(processed=True))

    def visit_list(self, node: ast.ListNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_object(self, node: ast.LiteralNode) -> Inspection:
        return Inspection(processed=True)

    def visit_dict_bundle(self, node: ast.DictBundleNode) -> Any:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_str_bundle(self, node: ast.StrBundleNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_var(self, node: ast.VarNode) -> Inspection:
        default = None if node.default is None else node.default.data
        variables = py_.set_({}, node.identifier.data, default)

        environ = {}
        if node.env is not None and node.env.data:
            environ[node.identifier.data] = default

        return Inspection(variables=variables, environ=environ)

    def visit_import(self, node: ast.ImportNode) -> Inspection:
        path = Path(node.path.data)
        if not path.is_absolute():
            path = self._cwd / path

        if path.exists():
            subdata = load(path)
            parsed = parse(subdata)

            old_cwd = self._cwd
            self._cwd = path.parent
            nested = parsed.accept(self)
            self._cwd = old_cwd
        else:
            warnings.warn(f"Cannot complete inspection: file {path} is missing.")
            nested = Inspection()

        return Inspection(imports={Path(path).resolve()}) + nested

    def visit_sweep(self, node: ast.SweepNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.cases], start=start)

    def visit_symbol(self, node: ast.SymbolNode) -> Any:
        return Inspection(symbols={str(node.symbol.data)})

    def visit_instance(self, node: ast.InstanceNode) -> Inspection:
        return Inspection(symbols={str(node.symbol.data)}) + node.args.accept(self)

    def visit_model(self, node: ast.ModelNode) -> Inspection:
        return self.visit_instance(node)

    def visit_for(self, node: ast.ForNode) -> Inspection:
        if node.identifier is not None:
            self._named_for_loops[node.identifier.data] = node.iterable.data
        iterable_insp = Inspection(variables=py_.set_({}, node.iterable.data, None))
        body_insp = node.body.accept(self)
        return iterable_insp + body_insp

    def visit_index(self, node: ast.IndexNode) -> Inspection:
        return Inspection()

    def visit_item(self, node: ast.ItemNode) -> Inspection:
        variables = {}
        if node.identifier is not None:
            loop_id, _, key = node.identifier.data.partition(".")
            iterable_name = self._named_for_loops[loop_id]
            full_path = f"{iterable_name}.{key}"
            py_.set_(variables, full_path, None)
        return Inspection(variables=variables)


def inspect(node: ast.Node, cwd: Optional[Path] = None) -> Inspection:
    inspector = Inspector(cwd=cwd)
    return node.accept(inspector)
