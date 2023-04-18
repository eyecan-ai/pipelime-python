import typing as t
import pydantic as pyd

from pipelime.stages import SampleStage
from pipelime.utils.pydantic_types import ItemType, YamlInput, CallableDef

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
        ..., alias="k", description="The metadata key in pydash dot notation."
    )
    value: YamlInput = pyd.Field(
        None, alias="v", description="The value to set, ie, any valid yaml/json value."
    )

    filter_query: t.Optional[str] = pyd.Field(
        None,
        alias="q",
        description=("A dictquery (cfr. https://github.com/cyberlis/dictquery)."),
    )
    filter_fn: t.Optional[CallableDef] = pyd.Field(
        None,
        alias="f",
        description=(
            "A `class.path.to.func` (or `file.py:func`) to "
            "a callable `(Sample) -> bool` returning True for any valid sample."
        ),
    )

    _filter = pyd.PrivateAttr(None)

    @pyd.validator("filter_fn", always=True)
    def _check_filters(
        cls, v: t.Optional[CallableDef], values: t.Mapping[str, t.Any]
    ) -> bool:
        fquery = values.get("filter_query", default=None)
        if (v is None) == (fquery is None):
            raise ValueError("You should define either `filter_query` or `filter_fn`")
        return True

    def _filter_key_fn(self, x):
        return x.match(self.filter_query)

    def __init__(self, **data):
        super().__init__(**data)

        self._filter = (
            self._filter_key_fn if self.filter_fn is None else self.filter_fn.value
        )

    def __call__(self, x: "Sample") -> "Sample":
        if self._filter(x):
            x = x.deep_set(self.key_path, self.value)
        return x
