from urllib.parse import ParseResult
import typing as t

from pipelime.sequences import Sample
from pipelime.stages import SampleStage
from pipelime.items import Item


class StageUploadToRemote(SampleStage):
    def __init__(self, *remotes: ParseResult, keys_to_upload: t.Optional[t.Collection[str]] = None):
        self._remotes = remotes
        self._keys_to_upload = keys_to_upload

    def __call__(self, x: Sample) -> Sample:
        for k, v in x.items():
            if not self._keys_to_upload or k in self._keys_to_upload:
                v.serialize(*self._remotes)
        return x


class StageRemoveRemote(SampleStage):
    def __init__(
        self,
        *always_remove: ParseResult,
        **remove_by_key: t.Union[ParseResult, t.Sequence[ParseResult]]
    ):
        self._always_remove = list(always_remove)
        self._remove_by_key = {
            k: (list(v) if isinstance(v, t.Sequence) else [v])
            for k, v in remove_by_key.items()
        }

    def __call__(self, x: Sample) -> Sample:
        new_data: t.Dict[str, Item] = {}
        for k, v in x.items():
            rm = self._always_remove + self._remove_by_key.get(k, [])  # type: ignore
            new_data[k] = v.remove_data_source(rm)
        return Sample(new_data)
