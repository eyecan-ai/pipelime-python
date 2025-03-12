import hashlib
import pickle
import typing as t

import pydantic.v1 as pyd

# if t.TYPE_CHECKING:
from pipelime.sequences import Sample
from pipelime.stages import SampleStage
from pipelime.utils.pydantic_types import ItemType, YamlInput


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


class StageCopyItems(SampleStage, title="copy-items", arbitrary_types_allowed=True):
    """Copies items from a source sample to all samples of a sequence."""

    source: "Sample" = pyd.Field(..., description="The source sample.")
    key_list: t.Sequence[str] = pyd.Field(
        ..., alias="k", description="The keys of the items to copy."
    )
    force_shared: bool = pyd.Field(
        False,
        alias="f",
        description=("If True, the items will be copied as shared items"),
    )

    def __call__(self, x: "Sample") -> "Sample":
        for key in self.key_list:
            # If the key is not in the source, skip it.
            item = self.source[key]

            # If the item is not shared and the force_shared flag is set, make a new
            # item with the same values but shared.
            if self.force_shared and not item.is_shared:
                item = item.__class__.make_new(item, shared=True)

            # Set the item in the sample.
            x = x.set_item(key, item)

        return x


class StageShareItems(SampleStage, title="share-items"):
    """Sets a set of items in a sample as shared or non-shared."""

    share: t.Sequence[str] = pyd.Field(
        [], description="The keys of the items to set as shared."
    )
    unshare: t.Sequence[str] = pyd.Field(
        [], description="The keys of the items to set as non-shared."
    )

    # We need to check that the keys are not present in both lists.
    @pyd.root_validator
    def _validate_keys(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        share = values.get("share", [])
        unshare = values.get("unshare", [])
        if set(share) & set(unshare):
            raise ValueError(
                "The keys in the `share` and `unshare` lists must be disjoint."
            )
        return values

    def __call__(self, x: "Sample") -> "Sample":
        for key, item in x.items():
            if not item.is_shared and key in self.share:
                x = x.set_item(key, item.__class__.make_new(item, shared=True))
            elif item.is_shared and key in self.unshare:
                x = x.set_item(key, item.__class__.make_new(item, shared=False))
        return x
