import pydantic as pyd

import pipelime.sequences as pls
from pipelime.items import Item
from pipelime.sequences.pipes.base import PipedSequenceBase
import typing as t


@pls.piped_sequence
class ValidatedSequence(
    PipedSequenceBase, title="validate_samples", arbitrary_types_allowed=True
):
    """Validates the source sequence against a schema."""

    sample_schema: t.Union[
        str, t.Type[pyd.BaseModel], t.Mapping[str, t.Union[str, t.Type[Item]]]
    ] = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a pydantic model where fields name are "
            "the sample keys and field types are the expected item classes. The model "
            "can be a pydantic model class or an explicit `key: item-class` mapping."
        ),
    )
    validators: t.Optional[t.Mapping[str, t.Union[str, t.Callable]]] = pyd.Field(
        None,
        description=(
            "When `sample_schema` is an explicit mapping, validators "
            "can be set for each field as a simple callable accepting the field value "
            "and returning a validated version, while raising exception in case of "
            "error."
        ),
    )
    ignore_extra_keys: bool = pyd.Field(
        True,
        description=(
            "If True, extra keys are ignored, otherwise their presence raises an error."
        ),
    )
    lazy: bool = pyd.Field(
        True, description="If True, samples will be validated only when accessed."
    )
    max_samples: int = pyd.Field(
        -1,
        description=(
            "When the validation is NOT lazy, "
            "only the slice `[0:max_samples]` is checked."
        ),
    )

    @pyd.validator("sample_schema")
    def import_schema(cls, v):
        from pipelime.choixe.utils.imports import import_symbol

        if isinstance(v, str):
            imported_v = import_symbol(v)
            if not issubclass(imported_v, pyd.BaseModel):
                raise ValueError(f"`{v}` is not a pydantic model.")
            v = imported_v
        elif isinstance(v, t.Mapping):
            v = {
                k: import_symbol(val) if isinstance(val, str) else val
                for k, val in v.items()
            }
            for k, val in v.items():
                if not isinstance(k, str) or not issubclass(val, Item):
                    raise ValueError(
                        "Model mapping must be a `key: item-class` mapping."
                    )
        else:
            if not issubclass(v, pyd.BaseModel):
                raise ValueError(f"`{v}` is not a pydantic model.")
        return v

    @pyd.validator("validators")
    def import_validators(cls, v):
        from pipelime.choixe.utils.imports import import_symbol

        if v is None:
            return None
        return {
            k: import_symbol(val) if isinstance(val, str) else val
            for k, val in v.items()
        }

    def __init__(self, **data):
        super().__init__(**data)
        if isinstance(self.sample_schema, t.Mapping):
            import pydantic as pyd

            class Config(pyd.BaseConfig):
                arbitrary_types_allowed = True
                extra = pyd.Extra.ignore if self.ignore_extra_keys else pyd.Extra.forbid

            _validators = None
            if self.validators is not None:
                _validators = {}
                for k, v in self.validators.items():

                    def _validator(cls, val):
                        return v(val)  # type: ignore

                    _validators[f"{k}_validator"] = pyd.validator(k)(_validator)

            self.sample_schema = pyd.create_model(
                "SampleSchema",
                __config__=Config,
                __validators__=_validators,  # type: ignore
                **self.sample_schema,  # type: ignore
            )

        if not self.lazy:
            for sample in self.source[0 : self.max_samples]:  # noqa: E203
                self._check_sample(sample)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        if self.lazy:
            self._check_sample(sample)
        return sample

    def _check_sample(self, sample: pls.Sample):
        # Throws if validation fails
        _ = self.sample_schema(**sample)  # type: ignore
