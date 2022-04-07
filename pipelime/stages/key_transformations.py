import typing as t

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


class StageRemap(SampleStage):
    def __init__(self, remap: t.Mapping[str, str], remove_missing: bool = True):
        """Remaps keys in sample

        :param remap: old_key:new_key dictionary remap
        :type remap: Mapping[str, str]
        :param remove_missing: if TRUE missing keys in remap will be removed in the
            output sample, defaults to True
        :type remove_missing: bool, optional
        """
        super().__init__()
        self._remap = remap
        self._remove_missing = remove_missing

    def __call__(self, x: Sample) -> Sample:
        for kold, knew in self._remap:
            x = x.rename_key(kold, knew)
        if self._remove_missing:
            x = x.extract_keys(*self._remap.keys())
        return x


class StageKeysFilter(SampleStage):
    def __init__(self, key_list: t.Sequence[str], negate: bool = False):
        """Filter sample keys

        :param key_list: list of keys to preserve
        :type key_list: List[str]
        :param negate: TRUE to delete input keys, FALSE delete all but keys
        :type negate: bool
        """
        super().__init__()
        self._keys = key_list
        self._negate = negate

    def __call__(self, x: Sample) -> Sample:
        return (
            x.remove_keys(*self._keys) if self._negate else x.extract_keys(*self._keys)
        )
