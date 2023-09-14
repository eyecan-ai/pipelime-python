import os
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
from pipelime.choixe.utils.io import load, PipelimeTmp
from pipelime.choixe.utils.rand import rand
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
                imports. If set to None, the `os.getcwd()` will be used.
                Defaults to None.
            allow_branching (bool, optional): Set to False to disable processing on
                branching nodes, like sweeps. All branching nodes will be simply
                unparsed. Defaults to True.
        """
        super().__init__()
        self._context = context if context is not None else {}
        self._cwd = cwd if cwd is not None else Path(os.getcwd())
        self._allow_branching = allow_branching

        self._loop_data: Dict[str, LoopInfo] = {}
        self._current_loop: Optional[str] = None

    def _branches(self, *branches: List[Any]) -> List[Any]:
        if len(branches) == 1:
            return branches[0]
        return list(product(*branches))

    def _repeat(self, data: Any, n: int) -> List[Any]:
        new_data = []
        for _ in range(n):
            new_data.extend(deepcopy(data))
        return new_data

    def visit_dict(self, node: ast.DictNode) -> List[Dict]:
        data = [{}]
        for k, v in node.nodes.items():
            branches = self._branches(k.accept(self), v.accept(self))
            new_data = self._repeat(data, len(branches))
            for i, d in enumerate(new_data):
                d[branches[i // len(data)][0]] = branches[i // len(data)][1]
            data = new_data
        return data

    def visit_list(self, node: ast.ListNode) -> List[List]:
        data = [[]]
        for x in node.nodes:
            branches = x.accept(self)
            new_data = self._repeat(data, len(branches))
            for i, d in enumerate(new_data):
                d.append(branches[i // len(data)])
            data = new_data
        return data

    def visit_literal(self, node: ast.LiteralNode) -> List[Any]:
        return [node.data]

    def visit_dict_bundle(self, node: ast.DictBundleNode) -> List[Dict]:
        data = [{}]
        for x in node.nodes:
            branches = x.accept(self)
            new_data = self._repeat(data, len(branches))
            for i in range(len(new_data)):
                new_data[i].update(branches[i // len(data)])
            data = new_data
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
        id_branches = node.identifier.accept(self)
        default_branches = node.default.accept(self) if node.default else [None]
        env_branches = node.env.accept(self) if node.env else [None]

        branches = self._branches(id_branches, default_branches, env_branches)
        data = []

        for id_, default, env in branches:
            var_value = None
            found = False

            if py_.has(self._context, id_):
                var_value = py_.get(self._context, id_)
                found = True
            if not found and env:
                value = os.getenv(id_)
                if value is not None:
                    var_value = value
                    found = True
            if not found and node.default:
                var_value = default
                found = True

            if not found:
                raise ChoixeProcessingError(f"Variable not found: `{id_}`")

            # Recursively process the variable value
            re_parsed = parse(var_value)
            re_processed = re_parsed.accept(self)

            data.extend(re_processed)

        return data

    def visit_import(self, node: ast.ImportNode) -> List[Any]:
        all_nested = []
        branches = node.path.accept(self)
        for path in branches:
            path = Path(path)
            if not path.is_absolute():
                path = self._cwd / path

            subdata = load(path)
            parsed = parse(subdata)

            old_cwd = self._cwd
            self._cwd = path.parent
            nested = parsed.accept(self)
            self._cwd = old_cwd

            all_nested.append(nested)

        return self._branches(*all_nested)

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
        branches = self._branches(symbol_branches, args_branches)
        return [import_symbol(s, cwd=self._cwd)(**a) for s, a in branches]

    def visit_model(self, node: ast.ModelNode) -> Any:
        symbol_branches = node.symbol.accept(self)
        args_branches = node.args.accept(self)
        branches = self._branches(symbol_branches, args_branches)
        return [import_symbol(s, cwd=self._cwd).parse_obj(a) for s, a in branches]

    def visit_for(self, node: ast.ForNode) -> List[Any]:
        if isinstance(node.iterable.data, str):
            iterable = py_.get(self._context, node.iterable.data)
        else:
            iterable = node.iterable.data

        if isinstance(iterable, int):
            iterable = list(range(iterable))

        if not isinstance(iterable, Iterable):
            if isinstance(node.iterable.data, str) and not py_.has(
                self._context, node.iterable.data
            ):
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
            res = None
            if branch:
                if isinstance(branch[0], (str, int, float, bool)):
                    res = "".join([str(item) for item in branch])
                elif isinstance(branch[0], list):
                    res = []
                    [res.extend(item) for item in branch]
                elif isinstance(branch[0], dict):
                    res = {}
                    [res.update(item) for item in branch]
                else:
                    raise ChoixeProcessingError(
                        f"Invalid loop body: {branch[0]} is not a valid type"
                    )
            branches[i] = res

        return branches

    def visit_switch(self, node: ast.SwitchNode) -> List[Any]:
        value_branches = node.value.accept(self)
        set_branches = [x[0].accept(self) for x in node.cases]
        body_branches = [x[1].accept(self) for x in node.cases]
        default_branches = node.default.accept(self) if node.default else [None]

        all_branches = self._branches(
            value_branches, *set_branches, *body_branches, default_branches
        )

        branches = []
        for branch in all_branches:
            varname = branch[0]

            # If the variable is not in the context, raise an error
            if not py_.has(self._context, varname):
                msg = f"Switch variable `{varname}` not found in context"
                raise ChoixeProcessingError(msg)

            # Get the value of the variable
            value = py_.get(self._context, varname)

            # Match the value to the correct case
            for i in range(len(node.cases)):
                set_ = branch[i + 1]

                # If the set is not iterable, make it a list
                if not isinstance(set_, Iterable) or isinstance(set_, str):
                    set_ = [set_]

                # If the value is in the set, use the corresponding branch and break
                if value in set_:
                    branches.append(branch[i + 1 + len(node.cases)])
                    break

            # If no case matched, use the default if available, otherwise raise an error
            else:
                if node.default is not None:
                    branches.append(branch[-1])
                else:
                    msg = f"Switch variable `{varname}`={value} did not match any case"
                    raise ChoixeProcessingError(msg)

        return branches

    def visit_index(self, node: ast.IndexNode) -> List[Any]:
        branches = (
            node.identifier.accept(self) if node.identifier else [self._current_loop]
        )
        return [self._loop_data[x].index for x in branches]  # type: ignore

    def visit_item(self, node: ast.ItemNode) -> List[Any]:
        branches = (
            node.identifier.accept(self) if node.identifier else [self._current_loop]
        )
        sep = "."
        items = []
        for branch in branches:
            loop_id, _, key = str(branch).partition(sep)  # type: ignore
            item = py_.get(self._loop_data[loop_id].item, f"{sep}{key}")
            items.append(item)
        return items

    def visit_uuid(self, node: ast.UuidNode) -> List[str]:
        return [uuid.uuid1().hex]

    def visit_date(self, node: ast.DateNode) -> List[str]:
        ts = datetime.now()
        branches = node.format.accept(self) if node.format else [None]
        dates = []
        for branch in branches:
            date = ts.strftime(branch) if branch else ts.isoformat()
            dates.append(date)
        return dates

    def visit_cmd(self, node: ast.CmdNode) -> List[str]:
        stdouts = []
        for branch in node.command.accept(self):
            subp = os.popen(str(branch))
            stdout = subp.read()
            stdouts.append(stdout)
        return stdouts

    def visit_tmp_dir(self, node: ast.TmpDirNode) -> Any:
        paths = []
        branches = node.name.accept(self) if node.name else [""]
        for branch in branches:
            path = PipelimeTmp.make_subdir(f"choixe_tmp/{branch}")
            paths.append(path.resolve().absolute().as_posix())
        return paths

    def visit_rand(self, node: ast.RandNode) -> Any:
        args_branches = [
            node.args[i].accept(self) if i < len(node.args) else [...] for i in range(3)
        ]
        n_branches = node.n.accept(self) if node.n else [...]
        pdf_branches = node.pdf.accept(self) if node.pdf else [...]
        branches = self._branches(*args_branches, n_branches, pdf_branches)
        randoms = []
        for branch in branches:
            args = [x for x in branch[:3] if x is not ...]
            kwargs = {}
            if branch[3] is not ...:
                kwargs["n"] = branch[3]
            if branch[4] is not ...:
                kwargs["pdf"] = branch[4]
            randoms.append(rand(*args, **kwargs))
        return randoms


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
