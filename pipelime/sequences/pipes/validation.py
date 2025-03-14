import pydantic.v1 as pyd

import pipelime.sequences as pls
from pipelime.sequences.pipes import PipedSequenceBase
from pipelime.utils.pydantic_types import SampleValidationInterface


@pls.piped_sequence
class ValidatedSequence(
    PipedSequenceBase, title="validate_samples", arbitrary_types_allowed=True
):
    """Validates the source sequence against a schema."""

    sample_schema: SampleValidationInterface = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a pydantic model where fields name are "
            "the sample keys and field types are the expected item classes."
        ),
    )

    def __init__(self, **data):
        super().__init__(**data)
        if not self.sample_schema.lazy:
            seq = (
                self.source
                if self.sample_schema.max_samples == 0
                else self.source[0 : self.sample_schema.max_samples]
            )
            for sample in seq:
                self._check_sample(sample)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        if self.sample_schema.lazy:
            self._check_sample(sample)
        return sample

    def _check_sample(self, sample: pls.Sample):
        from pydantic.v1 import ValidationError
        from pydantic.v1.error_wrappers import display_errors

        try:
            _ = self.sample_schema.schema_model(**sample)
        except ValidationError as e:
            errs = e.errors()
            raise ValueError(
                f"Sample schema validation failed for:\n{str(sample)}\n\n"
                f"Errors:\n{display_errors(errs)}"
            ) from e
