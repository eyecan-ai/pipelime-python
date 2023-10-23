from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import pydash as py_
from box import Box
from schema import Schema

from pipelime.choixe.ast.nodes import Node
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.common import deep_set_
from pipelime.choixe.utils.io import dump, load
from pipelime.choixe.visitors import Inspection, decode, inspect, process, walk


class XConfig(Box):
    """A configuration with superpowers!"""

    PRIVATE_KEYS = ["_cwd", "_schema"]
    """Keys to exclude in visiting operations"""

    def __init__(
        self,
        data: Optional[Mapping] = None,
        cwd: Optional[Path] = None,
        schema: Optional[Schema] = None,
    ):
        """Constructor for `XConfig`

        Args:
            data (Optional[Mapping], optional): Optional dictionary containing
                initial data. Defaults to None.
            cwd (Optional[Path], optional): An optional path with the current working
                directory to use when resolving relative imports. If set to None, the
                system current working directory will be used. Defaults to None.
            schema (Optional[Schema], optional): Python schema object used for
                validation. Defaults to None.
        """

        # options
        self._cwd = cwd
        self._schema = None

        data = data if data is not None else {}
        assert isinstance(
            data, Mapping
        ), f'XConfig can only contain mappings, found "{data.__class__.__name__}"'

        self.update(data)
        self.set_schema(schema)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.to_dict()}>"

    @classmethod
    def from_file(
        cls, path: Union[str, Path], schema: Optional[Schema] = None
    ) -> XConfig:
        """Factory method to create a `XConfig` from file.

        Args:
            path (Union[str, Path]): Path to a markup file from which to load the data
                schema (Optional[Schema], optional): Python schema object used for
                validation. Defaults to None.

        Returns:
            XConfig: The loaded `XConfig`
        """
        path = Path(path)
        return XConfig(data=load(path), cwd=path.parent, schema=schema)

    def get_schema(self) -> Optional[Schema]:
        """Getter for the configuration schema"""
        return self._schema

    def set_schema(self, s: Optional[Schema]) -> None:
        """Setter for the configuration schema"""
        assert s is None or isinstance(
            s, Schema
        ), "schema is not a valid Schema object!"
        self._schema = s

    def get_cwd(self) -> Optional[Path]:
        """Getter for the configuration cwd"""
        return self._cwd

    def copy(self) -> XConfig:
        """Prototype method to copy this `XConfig` object.

        Returns:
            XConfig: A deepcopy of this `XConfig`.
        """
        return XConfig(data=self.decode(), cwd=self.get_cwd(), schema=self.get_schema())

    def validate(self, replace: bool = True):
        """Validate internal schema, if any

        Args:
            replace (bool, optional): True to replace internal dictionary with
                force-validated fields (e.g. Schema.Use). Defaults to True.
        """

        schema = self.get_schema()
        if schema is not None:  # pragma: no branch
            new_dict = schema.validate(self.to_dict())
            if replace:
                self.update(new_dict)

    def is_valid(self) -> bool:
        """Check for schema validity

        Returns:
            bool: True for valid or no schema inside
        """
        schema = self.get_schema()
        if schema is not None:
            return schema.is_valid(self.to_dict())
        return True

    def save_to(self, filename: Union[str, Path]) -> None:
        """Save configuration to output file

        Args:
            filename (str): output filename
        """
        dump(self.decode(), Path(filename))

    def deep_get(
        self, full_key: Union[str, list], default: Optional[Any] = None
    ) -> Any:
        """Gets value based on full path key, ie, 'a.b.0.d' or ['a','b','0','d']

        Args:
            full_key (Union[str, list]): full path key in pydash notation.
            default (Optional[Any], optional): result in case the path is not present.
                Defaults to None.

        Returns:
            Any: The value at the specified path.
        """
        return py_.get(self, full_key, default=default)

    def deep_set(
        self,
        full_key: Union[str, list],
        value: Any,
        only_valid_keys: bool = True,
        append_values: bool = False,
    ) -> None:
        """Sets value based on full path key, ie, 'a.b.0.d' or ['a','b','0','d'])

        Args:
            full_key (Union[str, list]): Full path key in pydash notation.
            value (Any): The value to set.
            only_valid_keys (bool, optional): True to avoid set on not present keys.
                Defaults to True.
            append_values (bool, optional): Append (as lists) or replace values.
                Defaults to False.
        """

        if not only_valid_keys or py_.has(self, full_key):
            deep_set_(self, key_path=full_key, value=value, append=append_values)

    def deep_update(
        self, data: Dict, full_merge: bool = False, append_values: bool = False
    ) -> None:
        """Updates current confing in depth, based on keys of other input dictionary.
        It is used to replace nested keys with new ones, but can also be used as a merge
        of two completely different XConfig if ``full_merge=True``.

        Args:
            data (Dict): An other dictionary to use as data source.
            full_merge (bool, optional): False to replace only the keys that are
                actually present. Defaults to False.
            append_values (bool, optional): Append (as lists) or replace values.
                Defaults to False.
        """

        other_chunks = walk(parse(data))
        for key, new_value in other_chunks:
            self.deep_set(
                key,
                new_value,
                only_valid_keys=not full_merge,
                append_values=append_values,
            )

    def parse(self) -> Node:
        """Parse this object into a Choixe AST Node.

        Returns:
            Node: The parsed node.
        """
        sanitized = dict(self)
        [sanitized.pop(x) for x in self.PRIVATE_KEYS]
        return parse(sanitized)

    def decode(self) -> Dict:
        """Convert this XConfig to a plain python dictionary. Also converts some nodes
        like numpy arrays into plain lists and restores some directives, eg, `$model`.

        Returns:
            Dict: The decoded dictionary.
        """
        return decode(self.parse())

    def to_dict(self) -> Dict:
        """Convert this XConfig to a plain python dictionary with no value decoding.

        Returns:
            Dict: The plain dictionary.
        """
        return {
            k: v for k, v in super().to_dict().items() if k not in self.PRIVATE_KEYS
        }

    def walk(self) -> List[Tuple[List[Union[str, int]], Any]]:
        """Perform the walk operation on this XConfig.

        Returns:
            List[Tuple[List[Union[str, int]], Any]]: The walk output.
        """
        return walk(self.parse())

    def _process(
        self,
        context: Optional[Dict[str, Any]] = None,
        allow_branching: bool = True,
        ask_missing_vars: bool = False,
    ) -> List[XConfig]:
        data = self._unsafe_process(
            context=context,
            allow_branching=allow_branching,
            ask_missing_vars=ask_missing_vars,
        )
        return [
            XConfig(data=x, cwd=self.get_cwd(), schema=self.get_schema()) for x in data
        ]

    def _unsafe_process(
        self,
        context: Optional[Dict[str, Any]] = None,
        allow_branching: bool = True,
        ask_missing_vars: bool = False,
    ) -> List[Any]:
        return process(
            self.parse(),
            context=context,
            cwd=self.get_cwd(),
            allow_branching=allow_branching,
            ask_missing_vars=ask_missing_vars,
        )

    def process(
        self,
        context: Optional[Dict[str, Any]] = None,
        ask_missing_vars: bool = False,
    ) -> XConfig:
        """Process this XConfig without branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.
            ask_missing_vars (bool, optional): If True, ask the user to fill
                missing vars.

        Returns:
            XConfig: The processed XConfig.
        """
        return self._process(
            context=context,
            allow_branching=False,
            ask_missing_vars=ask_missing_vars,
        )[0]

    def unsafe_process(
        self,
        context: Optional[Dict[str, Any]] = None,
        ask_missing_vars: bool = False,
    ) -> Any:
        """Process this XConfig without branching and return the result, without
        wrapping it in an XConfig to allow for more flexibility in the result.

        No guarantees can be made about the result type, so it is up to the user to
        handle it properly.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.
            ask_missing_vars (bool, optional): If True, ask the user to fill
                missing vars.

        Returns:
            Any: The processed result.
        """
        return self._unsafe_process(
            context=context,
            allow_branching=False,
            ask_missing_vars=ask_missing_vars,
        )[0]

    def process_all(
        self,
        context: Optional[Dict[str, Any]] = None,
        ask_missing_vars: bool = False,
    ) -> List[XConfig]:
        """Process this XConfig with branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.
            ask_missing_vars (bool, optional): If True, ask the user to fill
                missing vars.

        Returns:
            List[XConfig]: A list of all processing outcomes.
        """
        return self._process(
            context=context,
            allow_branching=True,
            ask_missing_vars=ask_missing_vars,
        )

    def unsafe_process_all(
        self,
        context: Optional[Dict[str, Any]] = None,
        ask_missing_vars: bool = False,
    ) -> List[Any]:
        """Process this XConfig with branching and return the results, without
        wrapping them in XConfigs to allow for more flexibility in the result.

        No guarantees can be made about the result types, so it is up to the user to
        handle them properly.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.
            ask_missing_vars (bool, optional): If True, ask the user to fill
                missing vars.

        Returns:
            List[Any]: A list of all processing outcomes.
        """
        return self._unsafe_process(
            context=context,
            allow_branching=True,
            ask_missing_vars=ask_missing_vars,
        )

    def inspect(self) -> Inspection:
        return inspect(self.parse(), cwd=self.get_cwd())


class Choixe:
    """A lightweight wrapper that enables the use of Choixe on any python object,
    overcoming the limitations of the `XConfig` class, which is limited to the use of
    dictionaries as data source.

    Unlike `XConfig`, `Choixe` does not implement any python data structure, and
    delegates all access operations to the wrapped object. This means that it is
    possible to use `Choixe` on any python object, including dictionaries, lists,
    tuples, sets, strings, integers, floats, booleans, etc, as well as custom classes
    and instances.
    """

    def __init__(self, data: Any = None, cwd: Optional[Path] = None):
        """Constructor for `Choixe`

        Args:
            data (Any, optional): Optional object containing
                initial data, it can be anything. Defaults to None.
            cwd (Optional[Path], optional): An optional path with the current working
                directory to use when resolving relative imports. If set to None, the
                system current working directory will be used. Defaults to None.
        """

        self._cwd = cwd
        self._data = data

    def __repr__(self) -> str:
        return f"Choixe(data={self.data})"

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> Choixe:
        """Factory method to create a `Choixe` from file.

        Args:
            path (Union[str, Path]): Path to a markup file from which to load the data

        Returns:
            Choixe: The loaded `Choixe`
        """
        path = Path(path)
        return cls(data=load(path), cwd=path.parent)

    @property
    def cwd(self) -> Optional[Path]:
        """Getter for the configuration cwd"""
        return self._cwd

    @property
    def data(self) -> Any:
        """Getter for the configuration data"""
        return self._data

    def copy(self) -> Choixe:
        """Prototype method to copy this object.

        Returns:
            Choixe: A copy of this object
        """
        return Choixe(data=self.data, cwd=self.cwd)

    def save_to(self, filename: Union[str, Path]) -> None:
        """Save configuration to output file

        Args:
            filename (str): output filename
        """
        dump(self.decode(), Path(filename))

    def parse(self) -> Node:
        """Parse this object into a Choixe AST Node.

        Returns:
            Node: The parsed node.
        """
        return parse(self.data)

    def decode(self) -> Dict:
        """Convert this `Choixe` to a plain python dictionary. Also converts some nodes
        like numpy arrays into plain lists and restores some directives, eg, `$model`.

        Returns:
            Dict: The decoded dictionary.
        """
        return decode(self.parse())

    def walk(self) -> List[Tuple[List[Union[str, int]], Any]]:
        """Perform the walk operation on this `Choixe`.

        Returns:
            List[Tuple[List[Union[str, int]], Any]]: The walk output.
        """
        return walk(self.parse())

    def _process(
        self, context: Optional[Dict[str, Any]] = None, allow_branching: bool = True
    ) -> List[Choixe]:
        data = process(
            self.parse(), context=context, cwd=self.cwd, allow_branching=allow_branching
        )
        return [Choixe(data=x, cwd=self.cwd) for x in data]

    def process(self, context: Optional[Dict[str, Any]] = None) -> Choixe:
        """Process this Choixe without branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.

        Returns:
            `Choixe`: The processed Choixe.
        """
        return self._process(context=context, allow_branching=False)[0]

    def process_all(self, context: Optional[Dict[str, Any]] = None) -> List[Choixe]:
        """Process this Choixe with branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
                containing all variables values. Defaults to None.

        Returns:
            List[Choixe]: A list of all processing outcomes.
        """
        return self._process(context=context, allow_branching=True)

    def inspect(self) -> Inspection:
        """Inspect this Choixe returning all variables and imports.

        Returns:
            Inspection: The inspection result.
        """
        return inspect(self.parse(), cwd=self.cwd)
