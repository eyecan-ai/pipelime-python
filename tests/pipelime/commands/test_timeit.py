import pytest
from .test_general_base import TestGeneralCommandsBase


class TestTimeIt(TestGeneralCommandsBase):
    @pytest.mark.parametrize("skip_first", [0, 1, 5])
    @pytest.mark.parametrize("max_samples", [1, 3, None])
    @pytest.mark.parametrize("repeat", [1, 5])
    @pytest.mark.parametrize("process_timer", [True, False])
    @pytest.mark.parametrize("write_out", [True, False])
    def test_timeit(
        self,
        minimnist_dataset,
        skip_first,
        max_samples,
        repeat,
        process_timer,
        write_out,
        tmp_path,
    ):
        import shutil

        from pipelime.commands import TimeItCommand
        from pipelime.stages import StageLambda

        # NB: process timer skips any `sleep` calls
        def _fake_proc_no_sleep(x):
            from time import process_time_ns

            start = process_time_ns()
            while process_time_ns() - start == 0:
                _ = [i for i in range(1000)]
            return x

        def _run_cmd(optdict, ge):
            cmd = TimeItCommand.parse_obj(optdict)
            cmd()
            assert cmd.average_time is not None
            if ge:
                assert cmd.average_time.nanosec >= 0
            else:
                assert cmd.average_time.nanosec > 0

        # input + operations + output
        out_folder = (tmp_path / "output").as_posix() if write_out else None
        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": out_folder,
            "operations": {
                "map": {
                    "stage": StageLambda(_fake_proc_no_sleep),
                }
            },
            "skip_first": skip_first,
            "max_samples": max_samples,
            "repeat": repeat,
            "process": process_timer,
            "clear_output_folder": repeat > 1 and write_out,
        }
        _run_cmd(params, False)

        # operations + output
        if out_folder:
            shutil.rmtree(out_folder, ignore_errors=True)
        params["input"] = None
        params["operations"] = {
            "toy_dataset": 10,
            "map": StageLambda(_fake_proc_no_sleep),
        }
        _run_cmd(params, False)

        # input + output
        if out_folder:
            shutil.rmtree(out_folder, ignore_errors=True)
        params["input"] = minimnist_dataset["path"].as_posix()
        params["operations"] = None
        _run_cmd(params, True)

        # input + operations
        params["operations"] = {"map": StageLambda(_fake_proc_no_sleep)}
        params["output"] = None
        _run_cmd(params, False)

        if write_out:
            # input
            params["operations"] = None
            _run_cmd(params, True)

            # operations
            params["input"] = None
            params["operations"] = {
                "toy_dataset": 10,
                "map": StageLambda(_fake_proc_no_sleep),
            }
            _run_cmd(params, False)

        params["input"] = None
        params["operations"] = None
        with pytest.raises(ValueError):
            _run_cmd(params, False)
