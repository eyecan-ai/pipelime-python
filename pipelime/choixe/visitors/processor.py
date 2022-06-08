import os
import tempfile
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pydash as py_

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.imports import import_symbol
from pipelime.choixe.utils.io import load
from pipelime.choixe.visitors.unparser import unparse


class ChoixeProcessingError(Exception):
    pass


@dataclass
class LoopInfo:
    index: int
    item: Any


class Processor(ast.NodeVisitor):
    """`NodeVisitor` that implements the processing logic of Choixe."""

    def __init__(
        self,
        context: Optional[Dict[str, Any]] = None,
        cwd: Optional[Path] = None,
        allow_branching: bool = True,
    ) -> None:
        """Constructor for `Processor`

        Args:
            context (Optional[Dict[str, Any]], optional): A data structure containing
            the values that will replace the variable nodes. Defaults to None.
            cwd (Optional[Path], optional): current working directory used for relative
            imports. If set to None, the `os.getcwd()` will be used. Defaults to None.
            allow_branching (bool, optional): Set to False to disable processing on
            branching nodes, like sweeps. All branching nodes will be simply unparsed.
            Defaults to True.
        """
        super().__init__()
        self._context = context if context is not None else {}
        self._cwd = cwd if cwd is not None else Path(os.getcwd())
        self._allow_branching = allow_branching

        self._loop_data: Dict[str, LoopInfo] = {}
        self._current_loop: Optional[str] = None
        self._tmp_name = uuid.uuid1().hex

    def visit_dict(self, node: ast.DictNode) -> List[Dict]:
        data = [{}]
        for k, v in node.nodes.items():
            branches = list(product(k.accept(self), v.accept(self)))
            new_data = []
            for _ in range(len(branches)):
                new_data.extend(deepcopy(data))
            for i, d in enumerate(new_data):
                d[branches[i // len(data)][0]] = branches[i // len(data)][1]
            data = new_data
        return data

    def visit_list(self, node: ast.ListNode) -> List[List]:
        data = [[]]
        for x in node.nodes:
            branches = x.accept(self)
            new_data = []
            for _ in range(len(branches)):
                new_data.extend(deepcopy(data))
            for i, d in enumerate(new_data):
                d.append(branches[i // len(data)])
            data = new_data
        return data

    def visit_object(self, node: ast.LiteralNode) -> List[Any]:
        return [node.data]

    def visit_dict_bundle(self, node: ast.DictBundleNode) -> List[Dict]:
        data = [{}]
        for x in node.nodes:
            branches = x.accept(self)
            N = len(data)
            data *= len(branches)
            for i in range(len(data)):
                data[i].update(branches[i // N])
        return data

    def visit_str_bundle(self, node: ast.StrBundleNode) -> List[str]:
        data = [""]
        for x in node.nodes:
            branches = x.accept(self)
            N = len(data)
            data *= len(branches)
            for i in range(len(data)):
                data[i] += str(branches[i // N])
        return data

    def visit_var(self, node: ast.VarNode) -> List[Any]:
        if py_.has(self._context, node.identifier.data):
            return [py_.get(self._context, node.identifier.data)]

        if node.env is not None and node.env.data:
            value = os.getenv(node.identifier.data)
            if value is not None:
                return [value]

        if node.default is not None:
            return [node.default.data]

        raise ChoixeProcessingError(f"Variable not found: `{node.identifier.data}`")

    def visit_import(self, node: ast.ImportNode) -> List[Any]:
        path = Path(node.path.data)
        if not path.is_absolute():
            path = self._cwd / path

        subdata = load(path)
        parsed = parse(subdata)

        old_cwd = self._cwd
        self._cwd = path.parent
        nested = parsed.accept(self)
        self._cwd = old_cwd

        return nested

    def visit_sweep(self, node: ast.SweepNode) -> List[Any]:
        if self._allow_branching:
            cases = []
            for x in node.cases:
                cases.extend(x.accept(self))
            return cases
        else:
            return [unparse(node)]

    def visit_symbol(self, node: ast.SymbolNode) -> List[Any]:
        branches = node.symbol.accept(self)
        return [import_symbol(s, cwd=self._cwd) for s in branches]

    def visit_instance(self, node: ast.InstanceNode) -> List[Any]:
        symbol_branches = node.symbol.accept(self)
        args_branches = node.args.accept(self)
        branches = list(product(symbol_branches, args_branches))
        return [import_symbol(s, cwd=self._cwd)(**a) for s, a in branches]

    def visit_model(self, node: ast.ModelNode) -> Any:
        symbol_branches = node.symbol.accept(self)
        args_branches = node.args.accept(self)
        branches = list(product(symbol_branches, args_branches))
        return [import_symbol(s, cwd=self._cwd).parse_obj(a) for s, a in branches]

    def visit_for(self, node: ast.ForNode) -> List[Any]:
        iterable = py_.get(self._context, node.iterable.data)
        if not isinstance(iterable, Iterable):
            if not py_.has(self._context, node.iterable.data):
                raise ChoixeProcessingError(
                    f"Loop variable `{node.iterable.data}` not found in context"
                )
            raise ChoixeProcessingError(
                f"Loop variable `{node.iterable.data}` is not iterable"
            )
        id_ = uuid.uuid1().hex if node.identifier is None else str(node.identifier.data)
        prev_loop = self._current_loop
        self._current_loop = id_

        branches: List[Any] = []
        for i, x in enumerate(iterable):
            self._loop_data[self._current_loop] = LoopInfo(i, x)
            branches.append(node.body.accept(self))

        self._current_loop = prev_loop

        branches = list(product(*branches))
        for i, branch in enumerate(branches):
            if isinstance(node.body, ast.DictNode):
                res = {}
                [res.update(item) for item in branch]
            elif isinstance(node.body, ast.ListNode):
                res = []
                [res.extend(item) for item in branch]
            else:
                res = "".join([str(item) for item in branch])
            branches[i] = res

        return branches

    def visit_index(self, node: ast.IndexNode) -> List[Any]:
        id_ = (
            self._current_loop if node.identifier is None else str(node.identifier.data)
        )
        return [self._loop_data[id_].index]  # type: ignore

    def visit_item(self, node: ast.ItemNode) -> List[Any]:
        key = (
            self._current_loop if node.identifier is None else str(node.identifier.data)
        )
        sep = "."
        loop_id, _, key = key.partition(sep)  # type: ignore
        return [py_.get(self._loop_data[loop_id].item, f"{sep}{key}")]

    def visit_uuid(self, node: ast.UuidNode) -> List[str]:
        return [uuid.uuid1().hex]

    def visit_date(self, node: ast.DateNode) -> List[str]:
        format_ = node.format
        ts = datetime.now()
        if format_ is None:
            return [ts.isoformat()]
        else:
            return [ts.strftime(format_.data)]

    def visit_cmd(self, node: ast.CmdNode) -> List[str]:
        subp = os.popen(node.command.data)
        return [subp.read()]

    def visit_tmp_dir(self, node: ast.TmpDirNode) -> Any:
        name = self._tmp_name if node.name is None else node.name.data
        path = Path(tempfile.gettempdir()) / name
        path.parent.mkdir(exist_ok=True, parents=True)
        return [str(path)]


def process(
    node: ast.Node,
    context: Optional[Dict[str, Any]] = None,
    cwd: Optional[Path] = None,
    allow_branching: bool = True,
) -> Any:
    """Processes a Choixe AST node into a list of all possible outcomes.

    Args:
        node (Node): The AST node to process.
        context (Optional[Dict[str, Any]], optional): A data structure containing
        the values that will replace the variable nodes. Defaults to None.
        cwd (Optional[Path], optional): current working directory used for relative
        imports. If set to None, the `os.getcwd()` will be used. Defaults to None.
        allow_branching (bool, optional): Set to False to disable processing on
        branching nodes, like sweeps. All branching nodes will be simply unparsed.
        Defaults to True.

    Returns:
        Any: The list of all possible outcomes. If branching is disabled, the list will
        have length 1.
    """
    processor = Processor(context=context, cwd=cwd, allow_branching=allow_branching)
    return node.accept(processor)
