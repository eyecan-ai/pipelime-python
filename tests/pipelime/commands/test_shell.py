import pytest
import platform


class TestShell:
    def _base_file_test(self, command, inputs, outputs, out_path, expected_content):
        from pipelime.commands import ShellCommand

        cmd = ShellCommand(command=command, inputs=inputs, outputs=outputs)  # type: ignore
        cmd()
        with open(out_path) as f:
            assert expected_content.strip().lower() == f.read().strip().lower()

        assert (
            cmd.command_name
            == f"{ShellCommand.command_title()}:{command.partition(' ')[0]}"
        )
        assert cmd.get_inputs() == inputs
        assert cmd.get_outputs() == outputs

    @pytest.mark.skipif(platform.system() != "Windows", reason="Windows only")
    def test_shell_on_win(self, minimnist_dataset, tmp_path):
        from pipelime.sequences import SamplesSequence

        seq = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        f1 = seq[0]["metadata"].local_sources[0].resolve().absolute()
        f2 = seq[1]["metadata"].local_sources[0].resolve().absolute()
        outfile = tmp_path / "out.txt"

        expected_content = f"""
Comparing files {f1} and {f2}
***** {f1}
    1:  {{
    2:      "sample_id": 0,
    3:      "double": 0.0,
    4:      "half": 0.0,
    5:      "random": 0.9947
    6:  }}
***** {f2}
    1:  {{
    2:      "sample_id": 1,
    3:      "double": 2.0,
    4:      "half": 0.5,
    5:      "random": 0.132
    6:  }}
*****
"""

        self._base_file_test(
            command="FC /L /N {f1} {f2} > {out}",
            inputs={"f1": f1, "f2": f2},
            outputs={"out": str(outfile)},
            out_path=outfile,
            expected_content=expected_content,
        )

    @pytest.mark.skipif(platform.system() != "Linux", reason="Linux only")
    def test_shell_on_linux(self, minimnist_dataset, tmp_path):
        from pipelime.sequences import SamplesSequence

        seq = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        f1 = seq[0]["metadata"].local_sources[0].resolve().absolute()
        f2 = seq[1]["metadata"].local_sources[0].resolve().absolute()
        outfile = tmp_path / "out.txt"

        expected_content = """
17  60 0     62 2
36  60 0     65 5
55  71 9     61 1
56  71 9     63 3
57  64 4     62 2
58  67 7     12 ^J
59  12 ^J   175 }
60 175 }     12 ^J
"""

        self._base_file_test(
            command="cmp --ignore-initial 20 --print-chars -l -c {f1} {f2} > {out}",
            inputs={"f1": f1, "f2": f2},
            outputs={"out": str(outfile)},
            out_path=outfile,
            expected_content=expected_content,
        )

    @pytest.mark.parametrize("add_sharp", [True, False])
    def test_shell_python_script(self, shell_cmd, add_sharp, tmp_path):
        numbers = [2, 4, 5]
        outfile = tmp_path / "out.txt"

        prefix = "#" if add_sharp else ""
        expected_content = "\n".join(prefix + str(n) for n in numbers)

        self._base_file_test(
            command=f"python {shell_cmd}",
            inputs={"number": numbers, "add_sharp": add_sharp},
            outputs={"output": str(outfile)},
            out_path=outfile,
            expected_content=expected_content,
        )

    def test_shell_invalid(self):
        from pipelime.commands import ShellCommand

        cmd = ShellCommand(command="pippo", inputs={"mapping": {"a": 1}})  # type: ignore
        with pytest.raises(NotImplementedError):
            cmd()
