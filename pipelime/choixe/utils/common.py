import re
import typing as t

# pydash's string path delimiter (the same in pydash 8.0 and 8.1): an unescaped `.`,
# or a `[<int>]` list index (captured, so that it is kept as a path part)
_PYDASH_PATH_DELIM = re.compile(r"(?<!\\)(?:\\\\)*\.|(\[-?\d+\])")


def pydash_path(key_path: t.Any) -> t.Any:
    r"""A key path for pydash (``get``, ``set_``, ``has``, ``to_path``) addressing the
    same value on every pydash version.

    pydash 8.1 reads the empty key of a leading, trailing or doubled ``.`` in a string
    path (``".a"`` is ``["", "a"]``, ``"."`` is ``["", ""]``), where earlier versions
    dropped it (``["a"]``, ``[]``: the whole object). pipelime keeps the pre-8.1 meaning:
    the empty keys of a dotted or indexed string path are dropped here, and the other
    parts, escapes included, are joined back into a canonical path (``"a.b[0].c"``) that
    every pydash version splits the same way. A path left with a single key is returned
    unescaped (pydash takes a plain key as is), with no key as ``[]``. Any other path (a
    plain key without ``.`` or ``[``, a key list, an int) is returned unchanged.
    """
    if not (isinstance(key_path, str) and ("." in key_path or "[" in key_path)):
        return key_path
    # `re.split` alternates the text between two delimiters (a key, "" if empty) and
    # the captured group (a `[<int>]` list index, None for a `.`)
    parts = _PYDASH_PATH_DELIM.split(key_path)
    path = ""
    for i, part in enumerate(parts):
        if part:
            path += part if i % 2 or not path else "." + part
    if not path:
        return []
    if "." in path or "[" in path:
        return path
    return path.replace("\\\\", "\\")  # pydash's unescape; no `\.` is left here


class _RefItem:
    def __init__(self, parent, idx):
        self._parent = parent
        self._key = idx

    def __repr__(self):
        return f"_RefItem({self._parent}, {self._key})"

    def __getitem__(self, key):
        return self._parent[key] if self._key is None else self._parent[self._key][key]

    def __setitem__(self, key, val):
        if self._key is None:
            self._parent[key] = val
        else:
            self._parent[self._key][key] = val

    def __contains__(self, key):
        return (
            key in self._parent if self._key is None else key in self._parent[self._key]
        )

    def __len__(self):
        return len(self._parent) if self._key is None else len(self._parent[self._key])

    def extend(self, val):
        self._parent.extend(val) if self._key is None else self._parent[
            self._key
        ].extend(val)

    def is_none(self):
        return (
            (self._parent is None)
            if self._key is None
            else (self._parent[self._key] is None)
        )

    def set_(self, val):
        if self._key is None:
            self._parent = val
        else:
            self._parent[self._key] = val

    def get_(self):
        return self._parent if self._key is None else self._parent[self._key]


def deep_set_(  # noqa: C901
    obj: t.Union[t.Sequence, t.Mapping],
    key_path: t.Union[str, t.Sequence],
    value: t.Any,
    append: bool = False,
    default_sequence_factory=list,
    default_mapping_factory=dict,
):
    """Sets value based on full path key, ie, 'a.b.0.d' or 'a.b[0].d'. Note that the `.`
    refers to mapping keys of type string, while the `[]` indexes list elements.

    Args:
        obj (Union[Sequence, Mapping]): The object to update in place.
        key_path (Union[str, Sequence]): Full path key in pydash notation.
        value (Any): The value to set.
        append (bool, optional): If the key path is present, append the value instead of
            overwriting the current one. Defaults to False.
        default_sequence_factory (optional): a callable to create a new sequence object.
            Defaults to list.
        default_mapping_factory (optional): a callable to create a new mapping object.
            Defaults to dict.
    """
    from pydash.utilities import to_path

    if key_path is not None:  # pragma: no branch
        key_path_tokens = to_path(pydash_path(key_path))
        parent_node = _RefItem(obj, None)
        for tk in key_path_tokens:
            if isinstance(tk, int):
                if parent_node.is_none():
                    parent_node.set_(default_sequence_factory())
                if len(parent_node) <= tk:
                    parent_node.extend([None] * (tk + 1 - len(parent_node)))
                parent_node = _RefItem(parent_node.get_(), tk)
            else:
                if parent_node.is_none():
                    parent_node.set_(default_mapping_factory())
                if tk not in parent_node:
                    parent_node[tk] = None
                parent_node = _RefItem(parent_node.get_(), tk)

        if append and not parent_node.is_none():
            curr_val = parent_node.get_()
            if isinstance(curr_val, (bytes, str)) or not isinstance(
                curr_val, t.Sequence
            ):
                curr_val = [curr_val]
            if isinstance(value, (bytes, str)) or not isinstance(value, t.Sequence):
                value = [value]
            value = default_sequence_factory(list(curr_val) + list(value))
        parent_node.set_(value)
