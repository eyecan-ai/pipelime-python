from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

import pydash as py_

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import load


@dataclass
class Inspection:
    imports: Set[Path] = field(default_factory=set)
    variables: Dict[str, Any] = field(default_factory=dict)
    help_strings: Dict[str, Any] = field(default_factory=dict)
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
            ),  # type: ignore
            environ={**self.environ, **other.environ},
            symbols={*self.symbols, *other.symbols},
            processed=self.processed and other.processed,
            help_strings=py_.merge_with(
                self.help_strings, other.help_strings, iteratee=self._iteratee
            ),  # type: ignore
        )


class Inspector(ast.NodeVisitor):
    def __init__(self, cwd: Optional[Path] = None) -> None:
        super().__init__()
        self._cwd = cwd if cwd is not None else Path(os.getcwd())
        self._named_for_loops: Dict[str, str] = {}

    def _ignore(self, node: ast.Node) -> Any:
        return Inspection(processed=True)

    def visit_dict(self, node: ast.DictNode) -> Inspection:
        inspections = []
        for k, v in node.nodes.items():
            inspections.append(k.accept(self))
            inspections.append(v.accept(self))
        return sum(inspections, start=Inspection(processed=True))

    def visit_list(self, node: ast.ListNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_literal(self, node: ast.LiteralNode) -> Inspection:
        return Inspection(processed=True)

    def visit_dict_bundle(self, node: ast.DictBundleNode) -> Any:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_str_bundle(self, node: ast.StrBundleNode) -> Inspection:
        start = Inspection(processed=True)
        return sum([x.accept(self) for x in node.nodes], start=start)

    def visit_var(self, node: ast.VarNode) -> Inspection:
        id_insp = node.identifier.accept(self)
        default_insp = (
            node.default.accept(self) if node.default else Inspection(processed=True)
        )
        env_insp = node.env.accept(self) if node.env else Inspection(processed=True)
        insp = id_insp + default_insp + env_insp

        if insp.processed:  # pragma: no branch
            from pipelime.choixe.visitors.unparser import unparse

            id_ = node.identifier.data  # type: ignore
            default = None if node.default is None else unparse(node.default)
            variables = py_.set_({}, id_, default)

            help_strings = {}
            if node.help is not None:
                help_strings = py_.set_({}, id_, node.help.data)

            environ = {}
            if node.env is not None and node.env.data:  # type: ignore
                environ[id_] = default

            insp = insp + Inspection(
                variables=variables, environ=environ, help_strings=help_strings
            )

        return insp

    def visit_import(self, node: ast.ImportNode) -> Inspection:
        insp = node.path.accept(self)

        if insp.processed:  # pragma: no branch
            path = Path(node.path.data)  # type: ignore
            if not path.is_absolute():  # pragma: no branch
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

            insp = Inspection(imports={Path(path).resolve()}) + nested + insp

        return insp

    def visit_sweep(self, node: ast.SweepNode) -> Inspection:
        start = Inspection()
        return sum([x.accept(self) for x in node.cases], start=start)

    def visit_symbol(self, node: ast.SymbolNode) -> Any:
        insp = node.symbol.accept(self)
        if insp.processed:  # pragma: no branch
            insp = insp + Inspection(symbols={str(node.symbol.data)})  # type: ignore
        return insp

    def visit_instance(self, node: ast.InstanceNode) -> Inspection:
        insp = node.symbol.accept(self)
        if insp.processed:
            insp = insp + Inspection(symbols={str(node.symbol.data)})  # type: ignore
        return insp + node.args.accept(self)

    def visit_model(self, node: ast.ModelNode) -> Inspection:
        return self.visit_instance(node)

    def visit_for(self, node: ast.ForNode) -> Inspection:
        if node.identifier is not None:  # pragma: no branch
            self._named_for_loops[node.identifier.data] = node.iterable.data
        iterable_insp = (
            Inspection(variables=py_.set_({}, node.iterable.data, None))
            if isinstance(node.iterable.data, str)
            else Inspection(processed=True)
        )
        body_insp = node.body.accept(self)
        return iterable_insp + body_insp

    def visit_switch(self, node: ast.SwitchNode) -> Inspection:
        insp = node.value.accept(self)
        if insp.processed:  # pragma: no branch
            insp = Inspection(variables=py_.set_({}, node.value.data, None))  # type: ignore
        cases_insp = sum(
            [x.accept(self) + y.accept(self) for x, y in node.cases],
            start=Inspection(processed=True),
        )
        default_insp = (
            node.default.accept(self) if node.default else Inspection(processed=True)
        )
        return insp + cases_insp + default_insp

    def visit_index(self, node: ast.IndexNode) -> Inspection:
        insp = Inspection()
        if node.identifier is not None:  # pragma: no branch
            insp = insp + node.identifier.accept(self)
        return insp

    def visit_item(self, node: ast.ItemNode) -> Inspection:
        insp = Inspection()
        variables = {}
        if node.identifier is not None:  # pragma: no branch
            sub_insp = node.identifier.accept(self)
            if sub_insp.processed:  # pragma: no branch
                loop_id, _, key = node.identifier.data.partition(".")  # type: ignore
                if key:
                    iterable_name = self._named_for_loops[loop_id]
                    full_path = f"{iterable_name}.{key}"
                    py_.set_(variables, full_path, None)
            insp = insp + sub_insp + Inspection(variables=variables)
        return insp

    def visit_rand(self, node: ast.RandNode) -> Any:
        insp = Inspection()
        for arg in node.args:
            insp = insp + arg.accept(self)
        if node.n:
            insp = insp + node.n.accept(self)
        if node.pdf:
            insp = insp + node.pdf.accept(self)
        return insp


def inspect(node: ast.Node, cwd: Optional[Path] = None) -> Inspection:
    inspector = Inspector(cwd=cwd)
    return node.accept(inspector)
