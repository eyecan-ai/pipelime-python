import pytest
from pydantic import ValidationError
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

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_pipe(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import PipeCommand
        from pipelime.stages import StageKeysFilter
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "operations": {
                "map": {
                    "stage": StageKeysFilter(  # type: ignore
                        key_list=minimnist_dataset["image_keys"]
                    )
                }
            },
            "grabber": {
                "num_workers": nproc,
                "prefetch": prefetch,
            },
        }

        cmd = PipeCommand.parse_obj(params)
        cmd()

        src = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        dst = SamplesSequence.from_underfolder(tmp_path / "output")
        assert len(src) == len(dst) == minimnist_dataset["len"]
        for s1, s2 in zip(src, dst):
            TestAssert.samples_equal(
                s1.extract_keys(*minimnist_dataset["image_keys"]), s2
            )

        params["operations"] = {}
        with pytest.raises(ValidationError):
            cmd = PipeCommand.parse_obj(params)

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_concat(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import ConcatCommand
        from pipelime.sequences import SamplesSequence

        # create three sequences
        src1p, src2p, src3p = tmp_path / "seq1", tmp_path / "seq2", tmp_path / "seq3"
        seq = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        slice_len = len(seq) // 3
        seq[:slice_len].to_underfolder(src1p).run()
        seq[slice_len : 2 * slice_len].to_underfolder(src2p).run()
        seq[2 * slice_len :].to_underfolder(src3p).run()

        params = {
            "inputs": [
                src3p.as_posix(),
                src2p.as_posix(),
                src1p.as_posix(),
            ],
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = ConcatCommand.parse_obj(params)
        cmd()

        src1 = SamplesSequence.from_underfolder(src1p)
        src2 = SamplesSequence.from_underfolder(src2p)
        src3 = SamplesSequence.from_underfolder(src3p)
        dest = SamplesSequence.from_underfolder(tmp_path / "output")
        assert (
            len(src1) + len(src2) + len(src3) == len(dest) == minimnist_dataset["len"]
        )
        for s1, s2 in zip(src3, dest):
            TestAssert.samples_equal(s1, s2)
        for s1, s2 in zip(src2, dest[len(src3) : len(src3) + len(src2)]):
            TestAssert.samples_equal(s1, s2)
        for s1, s2 in zip(src1, dest[len(src3) + len(src2) :]):
            TestAssert.samples_equal(s1, s2)

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_zip(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import ZipCommand
        from pipelime.sequences import SamplesSequence
        from pipelime.stages import StageKeyFormat

        # create three sequences
        src1p, src2p, src3p = tmp_path / "seq1", tmp_path / "seq2", tmp_path / "seq3"
        seq = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        slice_len = len(seq) // 3
        seq[:slice_len].to_underfolder(src1p).run()
        seq[slice_len : 2 * slice_len].to_underfolder(src2p).run()
        seq[2 * slice_len :].to_underfolder(src3p).run()

        params = {
            "inputs": [
                src3p.as_posix(),
                src2p.as_posix(),
                src1p.as_posix(),
            ],
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "key_format": ["s2_*", "_s1"],
        }
        cmd = ZipCommand.parse_obj(params)
        cmd()

        src1 = SamplesSequence.from_underfolder(src1p).map(
            StageKeyFormat(key_format="*_s1")  # type: ignore
        )
        src2 = SamplesSequence.from_underfolder(src2p).map(
            StageKeyFormat(key_format="s2_*")  # type: ignore
        )
        src3 = SamplesSequence.from_underfolder(src3p)
        dest = SamplesSequence.from_underfolder(tmp_path / "output")
        assert min(len(src1), len(src2), len(src3)) == len(dest)
        for s1, s2, s3, d in zip(src1, src2, src3, dest):
            TestAssert.samples_equal(s3 + s2 + s1, d)

        params["key_format"].append("s3_*")
        cmd = ZipCommand.parse_obj(params)
        with pytest.raises(ValueError):
            cmd()
