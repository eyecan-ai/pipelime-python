import typing as t

import pydantic as pyd

import pipelime.sequences as pls
from pipelime.sequences.pipes.base import PipedSequenceBase


@pls.piped_sequence
class ValidatedSequence(
    PipedSequenceBase, title="validate_samples", arbitrary_types_allowed=True
):
    """Validates the source sequence against a schema."""

    sample_schema: t.Type[pyd.BaseModel] = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a pydantic model where fields name are "
            "the sample keys and field types are the expected item classes."
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

    def __init__(self, **data):
        super().__init__(**data)
        if not self.lazy:
            for sample in self.source[0 : self.max_samples]:  # noqa: E203
                self._check_sample(sample)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        if self.lazy:
            self._check_sample(sample)
        return sample

    def _check_sample(self, sample: pls.Sample):
        from pydantic import ValidationError
        from pydantic.error_wrappers import display_errors

        try:
            _ = self.sample_schema(**sample)  # type: ignore
        except ValidationError as e:
            errs = e.errors()
            raise ValueError(
                f"Sample schema validation failed for:\n{str(sample)}\n\n"
                f"Errors:\n{display_errors(errs)}"
            )
