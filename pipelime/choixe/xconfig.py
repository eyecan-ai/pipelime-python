from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import pydash as py_
from box import Box
from schema import Schema

from pipelime.choixe.ast.nodes import Node
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import dump, load
from pipelime.choixe.utils.common import deep_set_
from pipelime.choixe.visitors import Inspection, decode, inspect, process, walk


class XConfig(Box):
    """A configuration with superpowers!"""

    PRIVATE_KEYS = ["_cwd", "_schema"]
    """Keys to exclude in visiting operations"""

    def __init__(
        self,
        data: Optional[Dict] = None,
        cwd: Optional[Path] = None,
        schema: Optional[Schema] = None,
    ):
        """Constructor for `XConfig`

        Args:
            data (Optional[Dict], optional): Optional dictionary containing
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

    def __repr__(self) -> str:  # pragma: no cover
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
        """Validate internal schema if any

        Args:
            replace (bool, optional): True to replace internal dictionary with
            force-validated fields (e.g. Schema.Use). Defaults to True.
        """

        schema = self.get_schema()
        if schema is not None:
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
        self, full_key: Union[str, list], value: Any, only_valid_keys: bool = True
    ) -> None:
        """Sets value based on full path key, ie, 'a.b.0.d' or ['a','b','0','d'])

        Args:
            full_key (Union[str, list]): Full path key in pydash notation.
            value (Any): The value to set.
            only_valid_keys (bool, optional): True to avoid set on not present keys.
            Defaults to True.
        """

        if not only_valid_keys or py_.has(self, full_key):
            deep_set_(self, key_path=full_key, value=value, append=False)

    def deep_update(self, data: Dict, full_merge: bool = False):
        """Updates current confing in depth, based on keys of other input dictionary.
        It is used to replace nested keys with new ones, but can also be used as a merge
        of two completely different XConfig if `full_merge`=True.

        Args:
            data (Dict): An other dictionary to use as data source.
            full_merge (bool, optional): False to replace only the keys that are
            actually present. Defaults to False.
        """

        other_chunks = walk(parse(data))
        for key, new_value in other_chunks:
            self.deep_set(key, new_value, only_valid_keys=not full_merge)

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
        self, context: Optional[Dict[str, Any]] = None, allow_branching: bool = True
    ) -> List[XConfig]:
        data = process(
            self.parse(),
            context=context,
            cwd=self.get_cwd(),
            allow_branching=allow_branching,
        )
        return [
            XConfig(data=x, cwd=self.get_cwd(), schema=self.get_schema()) for x in data
        ]

    def process(self, context: Optional[Dict[str, Any]] = None) -> XConfig:
        """Process this XConfig without branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
            containing all variables values. Defaults to None.

        Returns:
            XConfig: The processed XConfig.
        """
        return self._process(context=context, allow_branching=False)[0]

    def process_all(self, context: Optional[Dict[str, Any]] = None) -> List[XConfig]:
        """Process this XConfig with branching.

        Args:
            context (Optional[Dict[str, Any]], optional): Optional data structure
            containing all variables values. Defaults to None.

        Returns:
            List[XConfig]: A list of all processing outcomes.
        """
        return self._process(context=context, allow_branching=True)

    def inspect(self) -> Inspection:
        return inspect(self.parse(), cwd=self.get_cwd())
