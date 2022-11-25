import pytest
from ... import TestAssert


class TestGeneralCommands:
    minimnist_partial_schema = {
        "pose": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "image": {
            "class_path": "PngImageItem",
            "is_optional": False,
            "is_shared": False,
        },
    }
    minimnist_full_schema = {
        "cfg": {
            "class_path": "YamlMetadataItem",
            "is_optional": False,
            "is_shared": True,
        },
        "numbers": {
            "class_path": "NumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "pose": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "image": {
            "class_path": "ImageItem",
            "is_optional": False,
            "is_shared": False,
        },
        "label": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": False,
        },
        "mask": {
            "class_path": "ImageItem",
            "is_optional": False,
            "is_shared": False,
        },
        "metadata": {
            "class_path": "MetadataItem",
            "is_optional": False,
            "is_shared": False,
        },
        "points": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": False,
        },
    }

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [1, 2, 4])
    def test_clone(
        self,
        minimnist_dataset,
        lazy: bool,
        ignore_extra_keys: bool,
        nproc: int,
        prefetch: int,
        tmp_path,
    ):
        from pipelime.commands import CloneCommand
        from pipelime.sequences import SamplesSequence

        cmd = CloneCommand.parse_obj(
            {
                "input": {
                    "folder": minimnist_dataset["path"],
                    "schema": {
                        "sample_schema": TestGeneralCommands.minimnist_partial_schema
                        if ignore_extra_keys
                        else TestGeneralCommands.minimnist_full_schema,
                        "ignore_extra_keys": ignore_extra_keys,
                        "lazy": lazy,
                    },
                },
                "output": {
                    "folder": tmp_path / "output",
                    "serialization": {
                        "override": {"DEEP_COPY": None},
                        "disable": {"MetadataItem": ["HARD_LINK", "DEEP_COPY"]},
                        "keys": {"image": "HARD_LINK"},
                    },
                },
                "grabber": f"{nproc},{prefetch}",
            }
        )
        cmd()

        src = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        dst = SamplesSequence.from_underfolder(tmp_path / "output")
        assert len(src) == len(dst) == minimnist_dataset["len"]
        for s1, s2 in zip(src, dst):
            TestAssert.samples_equal(s1, s2)

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
