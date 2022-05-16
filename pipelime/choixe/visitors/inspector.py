from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

import pydash as py_

from pipelime.choixe.ast.nodes import (
    DictBundleNode,
    DictNode,
    ForNode,
    ImportNode,
    IndexNode,
    InstanceNode,
    ItemNode,
    ListNode,
    LiteralNode,
    ModelNode,
    Node,
    NodeVisitor,
    StrBundleNode,
    SweepNode,
    SymbolNode,
    VarNode,
)
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import load


@dataclass
class Inspection:
    imports: Set[Path] = field(default_factory=set)
    variables: Dict[str, Any] = field(default_factory=dict)
    environ: Dict[str, Any] = field(default_factory=dict)
    symbols: Set[str] = field(default_factory=set)
    processed: bool = False

    def __add__(self, other: Inspection) -> Inspection:
        return Inspection(
            imports={*self.imports, *other.imports},
            variables=py_.merge(self.variables, other.variables),
            environ={**self.environ, **other.environ},
            symbols={*self.symbols, *other.symbols},
            processed=self.processed and other.processed,
        )


class Inspector(NodeVisitor):
    def __init__(self, cwd: Optional[Path] = None) -> None:
        super().__init__()
        self._cwd = cwd if cwd is not None else Path(os.getcwd())

    def visit_dict(self, node: DictNode) -> Inspection:
        inspections = []
        for k, v in node.nodes.items():
            inspections.append(k.accept(self))
            inspections.append(v.accept(self))
        return sum(inspections, start=Inspection(processed=True))

    def visit_list(self, node: ListNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_object(self, node: LiteralNode) -> Inspection:
        return Inspection(processed=True)

    def visit_dict_bundle(self, node: DictBundleNode) -> Any:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_str_bundle(self, node: StrBundleNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_var(self, node: VarNode) -> Inspection:
        default = None if node.default is None else node.default.data
        variables = py_.set_({}, node.identifier.data, default)

        environ = {}
        if node.env is not None and node.env.data:
            environ[node.identifier.data] = default

        return Inspection(variables=variables, environ=environ)

    def visit_import(self, node: ImportNode) -> Inspection:
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

    def visit_sweep(self, node: SweepNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.cases], start=start)

    def visit_symbol(self, node: SymbolNode) -> Any:
        return Inspection(symbols={str(node.symbol.data)})

    def visit_instance(self, node: InstanceNode) -> Inspection:
        return Inspection(symbols={str(node.symbol.data)}) + node.args.accept(self)

    def visit_model(self, node: ModelNode) -> Inspection:
        return self.visit_instance(node)

    def visit_for(self, node: ForNode) -> Inspection:
        iterable_insp = Inspection(variables=py_.set_({}, node.iterable.data, None))
        body_insp = node.body.accept(self)
        return iterable_insp + body_insp

    def visit_index(self, node: IndexNode) -> Inspection:
        return Inspection()

    def visit_item(self, node: ItemNode) -> Inspection:
        return Inspection()


def inspect(node: Node, cwd: Optional[Path] = None) -> Inspection:
    inspector = Inspector(cwd=cwd)
    return node.accept(inspector)
