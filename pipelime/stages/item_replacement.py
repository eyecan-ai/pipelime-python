import hashlib
import pickle
import typing as t
import pydantic as pyd

from pipelime.stages import SampleStage
from pipelime.utils.pydantic_types import ItemType, YamlInput

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class StageReplaceItem(SampleStage, title="replace-item"):
    """Replaces items in sample preserving internal values."""

    key_item_map: t.Mapping[str, ItemType] = pyd.Field(
        ...,
        description=(
            "A mapping `key: item_cls` returning the new item class for the key."
        ),
    )

    def __call__(self, x: "Sample") -> "Sample":
        for key, item_cls in self.key_item_map.items():
            if key in x:
                old_item = x[key]
                x = x.set_item(
                    key, item_cls.value.make_new(old_item, shared=old_item.is_shared)
                )
        return x


class StageSetMetadata(SampleStage, title="set-meta"):
    """Sets metadata in samples."""

    key_path: str = pyd.Field(
        ..., description="The metadata key in pydash dot notation."
    )
    value: YamlInput = pyd.Field(
        None, description="The value to set, ie, any valid yaml/json value."
    )

    def __call__(self, x: "Sample") -> "Sample":
        return x.deep_set(self.key_path, self.value.value)


class StageSampleHash(SampleStage, title="sample-hash"):
    """Compute the hash of a sample based on the selected items."""

    algorithm: str = pyd.Field(
        "sha256",
        description=(
            "The hashing algorithm from `hashlib` to use. Only algorithms that"
            "do not require parameters are supported."
        ),
    )

    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        ...,
        description=(
            "The keys to use for comparison. All items selected must be equal to"
            "consider two samples as duplicates."
        ),
    )

    hash_key: str = pyd.Field("hash", description="The key to store the hash.")

    @pyd.validator("algorithm")
    def _validate_algorithm(cls, v: str) -> str:
        algorithms_with_parameters = ["shake_128", "shake_256"]
        if v not in hashlib.algorithms_available or v in algorithms_with_parameters:
            raise ValueError(f"Invalid algorithm: {v}")
        return v

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.items import YamlMetadataItem

        hash = self._compute_sample_hash(x)
        x = x.set_item(self.hash_key, YamlMetadataItem(hash))
        return x

    def _compute_item_hash(self, item: t.Any) -> str:
        return hashlib.new(self.algorithm, pickle.dumps(item)).hexdigest()

    def _compute_sample_hash(self, sample: "Sample") -> str:
        keys = [self.keys] if isinstance(self.keys, str) else self.keys
        return "".join([self._compute_item_hash(sample[k]()) for k in keys])  # type: ignore
