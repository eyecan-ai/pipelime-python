from pipelime.piper import PipelimeCommand
from pydantic import DirectoryPath, Field, ValidationError


class ResumeCommand(PipelimeCommand, title="resume", extra="allow"):
    """Resume a command from a checkpoint.
    Any extra field is forwarded to the original command line.
    """

    ckpt: DirectoryPath = Field(..., alias="c", description="The checkpoint folder")

    def run(self):
        import pipelime.cli.main as cli
        from pipelime.piper.checkpoint import LocalCheckpoint

        ckpt = LocalCheckpoint(folder=self.ckpt)
        try:
            cli_opts = cli.PlCliOptions.parse_obj(
                ckpt.read_data(cli.PlCliOptions._namespace, "", None)
            )
            cli_opts.command_args.extend(
                x
                for k, c in self.dict(exclude={"ckpt"}).items()
                for x in [f"+{k}", str(c)]
            )
        except ValidationError:
            raise RuntimeError("Invalid checkpoint")

        cli.run_with_checkpoint(cli_opts, ckpt)
