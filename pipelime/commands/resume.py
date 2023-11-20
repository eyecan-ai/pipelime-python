import typing as t
from pathlib import Path

from pydantic import DirectoryPath, Field, ValidationError, conint, validator

from pipelime.cli.utils import PipelimeUserAppDir
from pipelime.piper import PipelimeCommand


class ResumeCommand(
    PipelimeCommand, title="resume", extra="allow", no_default_checkpoint=True
):
    """Resume a command from a checkpoint.
    Any extra field is forwarded to the original command line as '+' option.
    """

    ckpt: t.Union[
        DirectoryPath, conint(ge=1, le=PipelimeUserAppDir.MAX_CHECKPOINTS - 1)  # type: ignore
    ] = Field(
        None,
        alias="c",
        description=(
            "The checkpoint folder, or the nth-last default checkpoint "
            f"(up to {PipelimeUserAppDir.MAX_CHECKPOINTS-1}). "
            "If None it loads the last default checkpoint."
        ),
    )

    @validator("ckpt", always=True)
    def _validate_ckpt(cls, v):
        if v is None:
            return PipelimeUserAppDir.last_checkpoint_path()
        if isinstance(v, int):
            return PipelimeUserAppDir.last_checkpoint_path(v)
        return v

    def run(self):
        import pipelime.cli.main as cli
        from pipelime.piper.checkpoint import LocalCheckpoint

        ckpt = LocalCheckpoint(folder=self.ckpt)
        try:
            cli_opts = cli.PlCliOptions.parse_obj(
                ckpt.read_data(cli.PlCliOptions._namespace, "", None)
            )

            flattened_extra = {}
            for k, v in self.dict(exclude={"ckpt"}).items():
                self._flatten_values(v, k, flattened_extra)
            cli_opts.command_args.extend(
                x for k, v in flattened_extra.items() for x in [f"+{k}", v]
            )
        except ValidationError:  # pragma: no cover
            raise RuntimeError("Invalid checkpoint")

        cli.run_with_checkpoint(cli_opts, ckpt)

    def _flatten_values(
        self, value: t.Any, parent_key: str, flattened_values: t.Dict[str, t.Any]
    ):
        """Flatten values as expected by pipelime cli."""

        if isinstance(value, t.Mapping):
            for k, v_child in value.items():
                self._flatten_values(v_child, f"{parent_key}.{k}", flattened_values)
        elif not isinstance(value, (str, bytes)) and isinstance(value, t.Sequence):
            for i, v_child in enumerate(value):
                self._flatten_values(v_child, f"{parent_key}[{i}]", flattened_values)
        else:
            if isinstance(value, Path):
                value = value.as_posix()
            flattened_values[parent_key] = str(value)
