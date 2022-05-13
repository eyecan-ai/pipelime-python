from typer.testing import CliRunner

from pipelime.cli.main import app


class TestCliBase:
    def test_info(self, extra_modules):
        for module_data in extra_modules:
            for cmd in module_data["operators"] + module_data["commands"]:
                runner = CliRunner()
                result = runner.invoke(
                    app, ["-m"] + [str(module_data["filepath"])] + ["info"] + [cmd]
                )
                assert result.exit_code == 0
                assert result.exception is None
                assert cmd in result.output
