from pipelime.sequences import SamplesSequence, Sample


class ProxySequenceBase(SamplesSequence):
    """Reasonable base implementation of `size` and `get_sample`."""

    def __init__(self, source: SamplesSequence):
        super().__init__()
        self._source = source

    @property
    def source(self) -> SamplesSequence:
        return self._source

    def size(self) -> int:
        return len(self._source)

    def get_sample(self, idx: int) -> Sample:
        return self._source[idx]
