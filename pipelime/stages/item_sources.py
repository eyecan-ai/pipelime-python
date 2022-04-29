from urllib.parse import ParseResult
from pathlib import Path
import typing as t
import pydantic as pyd

from pipelime.sequences import Sample
from pipelime.stages import SampleStage
from pipelime.items import Item


class StageUploadToRemote(SampleStage):
    """Uploads the sample to one or more remote servers."""

    remotes: t.Sequence[ParseResult] = pyd.Field(
        ..., description="The remote addresses and buckets."
    )
    keys_to_upload: t.Optional[t.Sequence[str]] = pyd.Field(
        None, description="The keys to upload. If None, all keys are uploaded."
    )

    @pyd.validator("remotes")
    def unique_remotes(cls, v):
        return tuple(set(v))

    def __call__(self, x: Sample) -> Sample:
        for k, v in x.items():
            if not self.keys_to_upload or k in self.keys_to_upload:
                v.serialize(*self.remotes)  # type: ignore
        return x


class StageForgetSource(SampleStage):
    """Removes data sources, ie, file paths or remotes, from items."""

    always_remove: t.Sequence[t.Union[Path, ParseResult]] = pyd.Field(
        [], description="This sources will be removed from any item."
    )
    remove_by_key: t.Mapping[str, t.Sequence[t.Union[Path, ParseResult]]] = pyd.Field(
        {}, description="Sources to be removed from specific sample keys."
    )

    def __init__(
        self,
        *always_remove: t.Union[Path, ParseResult],
        **remove_by_key: t.Union[
            t.Union[Path, ParseResult], t.Sequence[t.Union[Path, ParseResult]]
        ]
    ):
        remove_by_key = {
            k: (v,) if isinstance(v, (Path, ParseResult)) else tuple(v)
            for k, v in remove_by_key.items()
        }
        super().__init__(
            always_remove=always_remove, remove_by_key=remove_by_key  # type: ignore
        )

    def __call__(self, x: Sample) -> Sample:
        new_data: t.Dict[str, Item] = {}
        for k, v in x.items():
            rm = self.always_remove + self.remove_by_key.get(k, tuple())  # type: ignore
            new_data[k] = v.remove_data_source(*rm)
        return Sample(new_data)
