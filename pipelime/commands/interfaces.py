import typing as t
from pathlib import Path

import pydantic as pyd


class GrabberInterface(pyd.BaseModel):
    """Multiprocessing grabbing options."""

    num_workers: int = pyd.Field(
        0,
        description=(
            "The number of processes to spawn. If negative, "
            "the number of (logical) cpu cores is used."
        ),
    )
    prefetch: pyd.PositiveInt = pyd.Field(
        2, description="The number of samples loaded in advanced by each worker."
    )

    def grab_all(
        self,
        sequence,
        *,
        keep_order: bool,
        parent_node=None,
        track_message: str = "",
        sample_fn=None,
    ):
        from pipelime.sequences import Grabber, grab_all

        grabber = Grabber(
            num_workers=self.num_workers, prefetch=self.prefetch, keep_order=keep_order
        )
        size = len(sequence)
        track_fn = (
            None
            if parent_node is None
            else (lambda x: parent_node.track(x, size=size, message=track_message))
        )
        grab_all(grabber, sequence, track_fn=track_fn, sample_fn=sample_fn)


class InputDatasetInterface(pyd.BaseModel):
    """Input dataset options."""

    folder: pyd.DirectoryPath = pyd.Field(..., description="Dataset root folder.")
    merge_root_items: bool = pyd.Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )

    def create_reader(self):
        from pipelime.sequences import SamplesSequence

        return SamplesSequence.from_underfolder(  # type: ignore
            folder=self.folder, merge_root_items=self.merge_root_items, must_exist=True
        )

    def __str__(self) -> str:
        return str(self.folder)


class SerializationModeInterface(pyd.BaseModel):
    """Serialization modes for items and keys."""

    override: t.Mapping[str, t.Optional[t.Union[str, t.Sequence[str]]]] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific item types, "
            "eg, `{CREATE_NEW_FILE: [ImageItem, my.package.MyItem, "
            "my/module.py:OtherItem]}`. An empty value applies to all items."
        ),
    )
    disable: t.Mapping[str, t.Union[str, t.Sequence[str]]] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes disabled for specific item types, "
            "eg, `{ImageItem: HARD_LINK, my.package.MyItem: [SYM_LINK, DEEP_COPY]}`. "
            "The special key `_` applies to all items."
        ),
    )
    keys: t.Mapping[str, str] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific sample keys, "
            "eg, `{image: CREATE_NEW_FILE}`."
        ),
    )

    _overridden_modes_cms: t.List[t.ContextManager]
    _disabled_modes_cms: t.List[t.ContextManager]

    class Config:
        underscore_attrs_are_private = True

    def _get_class_list(
        self, cls_paths: t.Optional[t.Union[str, t.Sequence[str]]]
    ) -> list:
        from pipelime.choixe.utils.imports import import_symbol

        if not cls_paths:
            return []
        if isinstance(cls_paths, str):
            cls_paths = [cls_paths]
        cls_paths = [c if "." in c else f"pipelime.items.{c}" for c in cls_paths]
        return [import_symbol(c) for c in cls_paths]

    def __init__(self, **data):
        import pipelime.items as pli

        super().__init__(**data)

        self._overridden_modes_cms = [
            pli.item_serialization_mode(m, *self._get_class_list(c))
            for m, c in self.override.items()
        ]

        self._disabled_modes_cms = [
            pli.item_disabled_serialization_modes(m, *self._get_class_list(c))
            for c, m in self.disable.items()
        ]

    def __enter__(self):
        for cm in self._overridden_modes_cms:
            cm.__enter__()
        for cm in self._disabled_modes_cms:
            cm.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        for cm in self._disabled_modes_cms:
            cm.__exit__(exc_type, exc_value, traceback)
        for cm in self._overridden_modes_cms:
            cm.__exit__(exc_type, exc_value, traceback)


class OutputDatasetInterface(pyd.BaseModel):
    """Output dataset options."""

    folder: Path = pyd.Field(..., description="Dataset root folder.")
    zfill: t.Optional[int] = pyd.Field(None, description="Custom index zero-filling.")
    exists_ok: bool = pyd.Field(
        False, description="If False raises an error when `folder` exists."
    )
    serialization: SerializationModeInterface = pyd.Field(
        default_factory=SerializationModeInterface,
        description="Serialization modes for items and keys.",
    )

    def serialization_cm(self) -> t.ContextManager:
        return self.serialization

    def append_writer(self, sequence):
        return sequence.to_underfolder(
            folder=self.folder,
            zfill=self.zfill,
            exists_ok=self.exists_ok,
            key_serialization_mode=self.serialization.keys,
        )

    def __str__(self) -> str:
        return str(self.folder)
