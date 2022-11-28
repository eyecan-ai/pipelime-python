import ast
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Type, Union

import astunparse
import schema as S

import pipelime.choixe.ast.nodes as c_ast

DIRECTIVE_PREFIX = "$"
"""Prefix used at the start of all Choixe directives."""


class ChoixeParsingError(Exception):
    pass


class ChoixeSyntaxError(ChoixeParsingError):
    pass


class ChoixeTokenValidationError(ChoixeParsingError):
    pass


class ChoixeStructValidationError(ChoixeParsingError):
    pass


@dataclass
class Token:
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]


class Scanner:
    """Choixe Scanner of python str objects."""

    DIRECTIVE_RE = r"(?:\$[^\)\( \.\,\$]+\([^\(\)]*\))|(?:\$[^\)\( \.\,\$]+)|(?:[^\$]*)"
    """Regex used to check if a string is a Choixe directive."""

    def _scan_argument(
        self, py_arg: Union[ast.Constant, ast.Attribute, ast.Name]
    ) -> Any:
        if isinstance(py_arg, ast.Constant):
            return py_arg.value
        elif isinstance(py_arg, ast.Attribute):
            name = astunparse.unparse(py_arg).strip()  # Remove trailing newline
            return str(name)
        elif isinstance(py_arg, ast.Name):
            return str(py_arg.id)
        else:
            raise ChoixeSyntaxError(py_arg.__class__)

    def _scan_directive(self, code: str) -> Token:
        try:
            py_ast = ast.parse(f"_{code}")  # Add "_" to avoid conflicts with python
        except SyntaxError:
            raise ChoixeSyntaxError(code)

        assert isinstance(py_ast, ast.Module)
        py_expr = py_ast.body[0].value  # type: ignore

        if isinstance(py_expr, ast.Call):
            # Remove the added "_"
            token_name = py_expr.func.id[1:]  # type: ignore
            py_args = py_expr.args
            py_kwargs = py_expr.keywords
        elif isinstance(py_expr, ast.Name):
            token_name = py_expr.id[1:]
            py_args = []
            py_kwargs = {}
        else:
            raise ChoixeSyntaxError(code)

        args = []
        for py_arg in py_args:
            args.append(self._scan_argument(py_arg))  # type: ignore

        kwargs = {}
        for py_kwarg in py_kwargs:
            key, value = py_kwarg.arg, py_kwarg.value
            kwargs[key] = self._scan_argument(value)  # type: ignore

        return Token(token_name, args, kwargs)

    def scan(self, data: str) -> List[Token]:
        """Transforms a string into a list of parsed tokens.

        Args:
            data (str): The string to parse.

        Returns:
            List[Token]: The list of parsed tokens.
        """
        res = []
        tokens = re.findall(self.DIRECTIVE_RE, data)
        for token in tokens:
            if len(token) == 0:
                continue
            if token.startswith(DIRECTIVE_PREFIX):
                token = self._scan_directive(token[len(DIRECTIVE_PREFIX) :])
            else:
                token = Token("str", [token], {})
            res.append(token)

        return res


class Parser:
    """Choixe parser for all kind of python objects."""

    def __init__(self) -> None:
        self._scanner = Scanner()
        self._type_map = {
            dict: self._parse_dict,
            list: self._parse_list,
            tuple: self._parse_list,
            str: self._parse_str,
        }

        self._call_forms = {
            self._token_schema("var"): c_ast.VarNode,
            self._token_schema("import"): c_ast.ImportNode,
            self._token_schema("sweep"): c_ast.SweepNode,
            self._token_schema("index"): c_ast.IndexNode,
            self._token_schema("item"): c_ast.ItemNode,
            self._token_schema("uuid"): c_ast.UuidNode,
            self._token_schema("date"): c_ast.DateNode,
            self._token_schema("cmd"): c_ast.CmdNode,
            self._token_schema("tmp"): c_ast.TmpDirNode,
            self._token_schema("symbol"): c_ast.SymbolNode,
        }

        self._extended_and_special_forms = {
            S.Schema(
                {
                    self._token_schema("directive"): str,
                    self._token_schema("args"): list,
                    self._token_schema("kwargs"): dict,
                }
            ): self._parse_extended_form,
            S.Schema(
                {
                    self._token_schema("call"): str,
                    S.Optional(self._token_schema("args")): S.Optional(dict),
                }
            ): self._parse_instance,
            S.Schema(
                {
                    self._token_schema("model"): str,
                    S.Optional(self._token_schema("args")): dict,
                }
            ): self._parse_model,
        }

        self._key_value_forms = {
            S.Schema({self._token_schema("for"): object}): self._parse_for,
            S.Schema({self._token_schema("switch"): list}): self._parse_switch,
        }

    def _token_schema(
        self,
        name: str,
        args: Optional[OrderedDict[str, Type]] = None,
        kwargs: Optional[OrderedDict[str, Type]] = None,
    ) -> S.Schema:
        args = OrderedDict() if args is None else args
        kwargs = OrderedDict() if kwargs is None else kwargs

        def _validator(token: Union[str, Token]) -> bool:
            if isinstance(token, str):
                tokens = self._scanner.scan(token)
                if len(tokens) != 1:
                    return False
                token = tokens[0]

            if token.name != name:
                return False

            return True

        return S.Schema(_validator)

    def _key_value_pairs_by_token_name(
        self, data: Dict[str, Any]
    ) -> Dict[str, Tuple[Token, Any]]:
        res = {}
        for k, v in data.items():
            token = self._scanner.scan(k)[0]
            res[token.name] = (token, v)
        return res

    def _parse_extended_form(self, data: dict) -> c_ast.Node:
        pairs = self._key_value_pairs_by_token_name(data)
        token = Token(pairs["directive"][1], pairs["args"][1], pairs["kwargs"][1])
        return self._parse_token(token)

    def _parse_instance(self, data: dict) -> c_ast.InstanceNode:
        pairs = self._key_value_pairs_by_token_name(data)
        symbol = self.parse(pairs["call"][1])
        args = (
            self.parse(pairs["args"][1])
            if "args" in pairs
            else c_ast.DictNode(nodes={})
        )
        return c_ast.InstanceNode(symbol=symbol, args=args)  # type: ignore

    def _parse_model(self, data: dict) -> c_ast.ModelNode:
        pairs = self._key_value_pairs_by_token_name(data)
        symbol = self.parse(pairs["model"][1])
        args = (
            self.parse(pairs["args"][1])
            if "args" in pairs
            else c_ast.DictNode(nodes={})
        )
        return c_ast.ModelNode(symbol, args)  # type: ignore

    def _parse_for(self, loop: str, body: Any) -> c_ast.ForNode:
        token = self._scanner.scan(loop)[0]
        iterable = c_ast.LiteralNode(data=token.args[0])
        identifier = (
            token.args[1] if len(token.args) > 1 else token.kwargs.get("identifier")
        )
        identifier = c_ast.LiteralNode(data=identifier) if identifier else None
        return c_ast.ForNode(
            iterable=iterable, body=self.parse(body), identifier=identifier
        )

    def _parse_switch(self, switch: str, body: Any) -> c_ast.SwitchNode:
        token = self._scanner.scan(switch)[0]
        value = c_ast.LiteralNode(data=token.args[0])
        cases = []
        default = None
        for entry in body:
            pairs = self._key_value_pairs_by_token_name(entry)
            if "default" in pairs:
                default = self.parse(pairs["default"][1])
            elif "case" in pairs and "then" in pairs:
                case_set = self.parse(pairs["case"][1])
                case_body = self.parse(pairs["then"][1])
                cases.append((case_set, case_body))
            else:
                raise ChoixeStructValidationError(entry)
        return c_ast.SwitchNode(value=value, cases=cases, default=default)

    def _parse_dict(self, data: dict) -> Union[c_ast.DictNode, c_ast.DictBundleNode]:
        # Check if the dict is an extended or special form, in that case parse it and
        # return the result, no further checks are needed.
        for schema, fn in self._extended_and_special_forms.items():
            if schema.is_valid(data):
                try:
                    return fn(data)
                except Exception as e:
                    raise ChoixeStructValidationError(data) from e

        # Check for each entry if it is a key_value form, in that case parse it and add
        # it to the bundle. Keep track of what is left to parse.
        parsed_keyvalues = []
        parsed_other = {}
        for k, v in data.items():
            any_valid = False

            for schema, fn in self._key_value_forms.items():
                if schema.is_valid({k: v}):
                    parsed_keyvalues.append(fn(k, v))
                    any_valid = True
                    break

            if not any_valid:
                parsed_other[self._parse_str(k)] = self.parse(v)

        # Parse the remaining entries as a DictNode.
        parsed_other = c_ast.DictNode(nodes=parsed_other)

        # If no key_value form was found, return the DictNode.
        if len(parsed_keyvalues) == 0:
            return parsed_other
        # If there is only one key_value form and no remaining data, return it.
        elif len(parsed_other.nodes) == 0 and len(parsed_keyvalues) == 1:
            return parsed_keyvalues[0]
        # If there is more than one key_value form, return a DictBundleNode.
        else:
            return c_ast.DictBundleNode(*parsed_keyvalues, parsed_other)

    def _parse_list(self, data: list) -> c_ast.ListNode:
        return c_ast.ListNode(*[self.parse(x) for x in data])

    def _parse_token(self, token: Token) -> c_ast.Node:
        if token.name == "str":
            return c_ast.LiteralNode(data=token.args[0])

        try:
            for schema, fn in self._call_forms.items():
                if schema.is_valid(token):
                    args = [self.parse(x) for x in token.args]
                    kwargs = {k: self.parse(v) for k, v in token.kwargs.items()}
                    node = fn(*args, **kwargs)
                    return node
        except TypeError:
            raise ChoixeStructValidationError(token)

        raise ChoixeTokenValidationError(token)

    def _parse_str(self, data: str) -> c_ast.Node:
        nodes = []
        for token in self._scanner.scan(data):
            nodes.append(self._parse_token(token))

        if len(nodes) == 0:
            return c_ast.LiteralNode(data="")
        elif len(nodes) == 1:
            return nodes[0]
        else:
            return c_ast.StrBundleNode(*nodes)

    def parse(self, data: Any) -> c_ast.Node:
        """Recursively transforms an object into a visitable AST node.

        Args:
            data (Any): The object to parse.

        Returns:
            Node: The parsed Choixe AST node.
        """
        try:
            fn = c_ast.LiteralNode
            for type_, parse_fn in self._type_map.items():
                if isinstance(data, type_):
                    fn = parse_fn
                    break
            res = fn(data)  # type: ignore
            return res

        except (ChoixeSyntaxError, SyntaxError) as e:
            code = e.args[0]
            raise ChoixeParsingError(
                f'Error when parsing code "{code}" found in "{data}", expected either '
                "a compact form like $DIRECTIVE, or a call form like "
                "$DIRECTIVE(ARGS, KWARGS)."
            )
        except ChoixeTokenValidationError as e:
            token: Token = e.args[0]
            raise ChoixeParsingError(
                f'Token "{token}" found in "{data}" does not validate against any of '
                "the available call forms. Please check that the directive and "
                "argument names are spelled correctly and match the directive "
                "signature."
            )
        except ChoixeStructValidationError as e:
            expr = e.args[0]
            raise ChoixeParsingError(
                f'Structure "{expr}" does not validate against any of the available '
                "special or extended forms. Please check that the directive and "
                "argument names are spelled correctly and match the directive "
                "signature."
            )


def parse(data: Any) -> c_ast.Node:
    """Recursively transforms an object into a visitable AST node.

    Args:
        data (Any): The object to parse.

    Returns:
        Node: The parsed Choixe AST node.
    """
    return Parser().parse(data)
