import typing as t

from pipelime.items import Item


class BinaryItem(Item[bytes]):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".bin",)

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> bytes:
        return fp.read()

    @classmethod
    def encode(cls, value: bytes, fp: t.BinaryIO):
        fp.write(value)

    @classmethod
    def validate(cls, raw_data: t.Any) -> bytes:
        if not isinstance(raw_data, bytes):
            raise ValueError(f"{cls}: raw data must be bytes.")  # pragma: no cover
        return raw_data

    @classmethod
    def pl_pretty_data(cls, value: bytes) -> t.Any:
        from rich.text import Text

        return Text(value.hex(), overflow="ellipsis") + Text(f" ({len(value)} bytes)")
