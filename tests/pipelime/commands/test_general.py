import pytest
from pathlib import Path
from pydantic import ValidationError
from ... import TestAssert, TestUtils


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
    @pytest.mark.parametrize("skip_empty", [True, False])
    def test_clone(
        self,
        minimnist_dataset,
        lazy: bool,
        ignore_extra_keys: bool,
        nproc: int,
        prefetch: int,
        skip_empty: bool,
        tmp_path,
    ):
        import shutil
        from pipelime.commands import CloneCommand
        from pipelime.sequences import SamplesSequence

        partial_input = tmp_path / "partial_input"
        shutil.copytree(
            minimnist_dataset["path"],
            partial_input,
            ignore=shutil.ignore_patterns("*01_*", "*10_*"),
        )
        len_out = (
            minimnist_dataset["len"] - 2 if skip_empty else minimnist_dataset["len"]
        )

        cmd = CloneCommand.parse_obj(
            {
                "input": {
                    "folder": partial_input,
                    "skip_empty": skip_empty,
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

        if lazy and not skip_empty:
            # schema validation fails
            with pytest.raises(Exception) as exc_info:
                cmd()
            if nproc == 0:
                assert isinstance(exc_info.value, ValueError)
            return

        cmd()

        src = SamplesSequence.from_underfolder(partial_input)
        dst = SamplesSequence.from_underfolder(tmp_path / "output")
        assert len(src) == minimnist_dataset["len"]
        assert len(dst) == len_out

        iout = 0
        for iin in range(len(src)):
            if skip_empty and iin in [1, 10]:
                continue
            TestAssert.samples_equal(src[iin], dst[iout])
            iout += 1

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
        with pytest.raises(ValueError):
            cmd = ZipCommand.parse_obj(params)  # output exists

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize("keys", [None, ["metadata", "label"]])
    @pytest.mark.parametrize("slicing", [(None, None, None), (1, 5, 2), (-2, -7, -3)])
    def test_remotes(self, minimnist_dataset, nproc, prefetch, keys, slicing, tmp_path):
        from pipelime.remotes import make_remote_url
        from pipelime.commands import AddRemoteCommand, RemoveRemoteCommand
        from pipelime.sequences import SamplesSequence

        # data lakes
        remote_urls = [
            make_remote_url(
                scheme="file",
                host="localhost",
                bucket=(tmp_path / "rmbucket1"),
            ),
            make_remote_url(
                scheme="file",
                host="localhost",
                bucket=(tmp_path / "rmbucket2"),
            ),
        ]

        params = {
            "input": {
                "folder": minimnist_dataset["path"].as_posix(),
                "merge_root_items": False,
            },
            "output": (tmp_path / "output_add").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "remotes": [r.geturl() for r in remote_urls],
        }
        if keys is not None:
            params["keys"] = keys
        if slicing[0] is not None:
            params["start"] = slicing[0]
        if slicing[1] is not None:
            params["stop"] = slicing[1]
        if slicing[2] is not None:
            params["step"] = slicing[2]
        cmd = AddRemoteCommand.parse_obj(params)
        cmd()

        # check output
        outseq = SamplesSequence.from_underfolder(params["output"])
        srcseq = SamplesSequence.from_underfolder(
            minimnist_dataset["path"], merge_root_items=False
        )
        remote_paths = [Path(r._replace(netloc="").geturl()) for r in remote_urls]

        start = 0 if slicing[0] is None else slicing[0]
        stop = len(srcseq) if slicing[1] is None else slicing[1]
        step = 1 if slicing[2] is None else slicing[2]
        if start < 0:
            start = len(srcseq) + start
        if stop < 0:
            stop = len(srcseq) + stop
        slice_range = range(start, stop, step)

        assert len(outseq) == len(srcseq)
        for i, (s, o) in enumerate(zip(srcseq, outseq)):
            for k, v in o.items():
                if i in slice_range and (keys is None or k in keys):
                    assert len(v.remote_sources) == len(remote_paths)
                    for r in v.remote_sources:
                        assert Path(r.geturl()).parent in remote_paths
                else:
                    assert len(v.remote_sources) == 0
            TestAssert.samples_equal(s, o)

        # now remove all the remotes but the latest
        params["input"] = params["output"]
        params["output"] = (tmp_path / "output_remove").as_posix()
        del params["remotes"][-1]
        cmd = RemoveRemoteCommand.parse_obj(params)
        cmd()

        # check output
        outseq = SamplesSequence.from_underfolder(params["output"])
        remote_paths = remote_paths[-1]

        assert len(outseq) == len(srcseq)
        for i, (s, o) in enumerate(zip(srcseq, outseq)):
            for k, v in o.items():
                if i in slice_range and (keys is None or k in keys):
                    assert len(v.remote_sources) == 1
                    assert Path(v.remote_sources[0].geturl()).parent == remote_paths
                else:
                    assert len(v.remote_sources) == 0
            TestAssert.samples_equal(s, o)

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize("max_samples", [0, -10, 5])
    def test_validate(self, minimnist_dataset, nproc, prefetch, max_samples):
        import yaml
        import io
        import pydash as py_
        from pipelime.commands import ValidateCommand
        from pipelime.sequences import SamplesSequence
        from pipelime.commands.interfaces import SampleValidationInterface

        # compute the validation schema
        params = {
            "input": {"folder": minimnist_dataset["path"]},
            "max_samples": max_samples,
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = ValidateCommand.parse_obj(params)
        cmd()

        # apply the schema on the input dataset
        assert cmd.output_schema_def is not None
        outschema = repr(cmd.output_schema_def)
        outschema = yaml.safe_load(io.StringIO(outschema))
        sample_schema = py_.get(outschema, cmd.root_key_path)
        assert sample_schema is not None
        py_.set_(params, cmd.root_key_path, sample_schema)
        cmd = ValidateCommand.parse_obj(params)
        cmd()

        # validate using standard piping as well
        seq = SamplesSequence.from_underfolder(
            params["input"]["folder"]
        ).validate_samples(
            sample_schema=SampleValidationInterface.parse_obj(sample_schema)
        )
        seq.run(num_workers=nproc, prefetch=prefetch)

        # check the schema-to-cmdline converter
        params["root_key_path"] = ""
        cmd = ValidateCommand.parse_obj(params)
        cmd()
        assert cmd.output_schema_def is not None
        assert cmd.output_schema_def.schema_def == outschema["input"]["schema"]

        # test the dictionary flatting
        outschema = ValidateCommand.OutputCmdLineSchema(
            schema_def={"a": 42, "b": [True, [1, 2], {1: {"c": None}}]}
        )
        assert (
            repr(outschema) == "+a 42 +b[0] True +b[1][0] 1 +b[1][1] 2 +b[2].1.c None"
        )

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_map(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import MapCommand
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "stage": {"filter-keys": {"key_list": minimnist_dataset["image_keys"]}},
        }
        cmd = MapCommand.parse_obj(params)
        cmd()

        # check output
        outseq = SamplesSequence.from_underfolder(params["output"])
        srcseq = SamplesSequence.from_underfolder(params["input"])
        for o, s in zip(outseq, srcseq):
            assert set(o.keys()) == set(minimnist_dataset["image_keys"])
            for k, v in o.items():
                assert TestUtils.numpy_eq(v(), s[k]())

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_sort(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import SortCommand
        from pipelime.sequences import SamplesSequence

        def _check_output(path, deep_key):
            last_random = 0.0
            outseq = SamplesSequence.from_underfolder(path)
            for x in outseq:
                assert x.deep_get(deep_key) > last_random
                last_random = x.deep_get(deep_key)

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output_key").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "sort_key": "metadata.random",
        }
        cmd = SortCommand.parse_obj(params)
        cmd()
        _check_output(params["output"], params["sort_key"])

        params["output"] = (tmp_path / "output_fn").as_posix()
        params["sort_fn"] = f"{Path(__file__).with_name('helper.py')}:sort_fn"
        cmd = SortCommand.parse_obj(params)
        with pytest.raises(ValueError):
            cmd()

        del params["sort_key"]
        if nproc == 0:
            cmd = SortCommand.parse_obj(params)
            cmd()
            _check_output(params["output"], "metadata.random")
            with pytest.raises(ValueError):
                cmd = SortCommand.parse_obj(params)  # output exists
        else:
            del params["sort_fn"]
            cmd = SortCommand.parse_obj(params)
            with pytest.raises(ValueError):
                cmd()

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_filter(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import FilterCommand
        from pipelime.sequences import SamplesSequence

        def _check_output(path):
            outseq = SamplesSequence.from_underfolder(path)
            for x in outseq:
                assert x.deep_get("metadata.double") == 6

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output_key").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "filter_query": "`metadata.double` == 6",
        }
        cmd = FilterCommand.parse_obj(params)
        cmd()
        _check_output(params["output"])

        params["output"] = (tmp_path / "output_fn").as_posix()
        params["filter_fn"] = f"{Path(__file__).with_name('helper.py')}:filter_fn"
        with pytest.raises(ValueError):
            cmd = FilterCommand.parse_obj(params)

        del params["filter_query"]
        if nproc == 0:
            cmd = FilterCommand.parse_obj(params)
            cmd()
            _check_output(params["output"])
            with pytest.raises(ValueError):
                cmd = FilterCommand.parse_obj(params)  # output exists
        else:
            del params["filter_fn"]
            with pytest.raises(ValueError):
                cmd = FilterCommand.parse_obj(params)

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize(
        "indexes",
        [
            (12, None, None),
            (None, 12, None),
            (None, None, 3),
            (12, 15, None),
            (None, 18, 3),
            (12, None, 3),
            (12, 18, 3),
        ],
    )
    @pytest.mark.parametrize("shuffle", [42, False])
    def test_slice(
        self, minimnist_dataset, nproc, prefetch, indexes, shuffle, tmp_path
    ):
        self._slice_check(
            minimnist_dataset,
            nproc,
            prefetch,
            indexes,
            shuffle,
            tmp_path / "output_slice",
        )

        indexes = (indexes[1], indexes[0], indexes[2])
        indexes = tuple(None if i is None else -i for i in indexes)
        self._slice_check(
            minimnist_dataset,
            nproc,
            prefetch,
            indexes,
            shuffle,
            tmp_path / "output_slice_neg",
        )

    def _slice_check(
        self, minimnist_dataset, nproc, prefetch, indexes, shuffle, tmp_path
    ):
        import random
        from pipelime.commands import SliceCommand
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": tmp_path.as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "slice": ":".join(["" if x is None else str(x) for x in indexes]),
            "shuffle": shuffle,
        }
        cmd = SliceCommand.parse_obj(params)
        cmd()

        inseq = SamplesSequence.from_underfolder(params["input"])
        outseq = SamplesSequence.from_underfolder(params["output"])

        idxs = list(range(len(inseq)))
        if shuffle:
            rnd = random.Random(shuffle)
            rnd.shuffle(idxs)
        idxs = idxs[slice(*indexes)]
        assert len(outseq) == len(idxs)

        for i, s in zip(idxs, outseq):
            TestAssert.samples_equal(inseq[i], s)

    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_set_meta(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import SetMetadataCommand
        from pipelime.sequences import SamplesSequence

        def _check_output(path):
            outseq = SamplesSequence.from_underfolder(path)
            for x in outseq:
                if x.deep_get("metadata.double") == 6:
                    assert x.deep_get("metadata.the_answer") == "fourtytwo"

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output_key").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "filter_query": "`metadata.double` == 6",
            "key_path": "metadata.the_answer",
            "value": "fourtytwo",
        }
        cmd = SetMetadataCommand.parse_obj(params)
        cmd()
        _check_output(params["output"])

        params["output"] = (tmp_path / "output_fn").as_posix()
        params["filter_fn"] = f"{Path(__file__).with_name('helper.py')}:set_meta_fn"
        with pytest.raises(ValueError):
            cmd = SetMetadataCommand.parse_obj(params)

        del params["filter_query"]
        if nproc == 0:
            cmd = SetMetadataCommand.parse_obj(params)
            cmd()
            _check_output(params["output"])
            with pytest.raises(ValueError):
                cmd = SetMetadataCommand.parse_obj(params)  # output exists
        else:
            del params["filter_fn"]
            with pytest.raises(ValueError):
                cmd = SetMetadataCommand.parse_obj(params)

    @pytest.mark.parametrize(
        "keys,expected_length",
        [
            ["cfg", 1],
            ["numbers", 1],
            ["pose", 1],
            ["image", 20],
            ["label", 10],
            ["mask", 12],
            ["metadata", 20],
            ["points", 12],
            [["cfg", "image"], 20],
            [["cfg", "numbers", "pose"], 1],
            [["image", "label"], 20],
            [
                [
                    "cfg",
                    "numbers",
                    "pose",
                    "image",
                    "label",
                    "mask",
                    "metadata",
                    "points",
                ],
                20,
            ],
        ],
    )
    @pytest.mark.parametrize("algorithm", ["sha256"])
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_filter_duplicates(
        self,
        minimnist_dataset,
        keys,
        expected_length,
        algorithm,
        nproc,
        prefetch,
        tmp_path,
    ):
        from pipelime.sequences import SamplesSequence
        from pipelime.commands import FilterDuplicatesCommand

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "keys": keys,
            "algorithm": algorithm,
        }
        cmd = FilterDuplicatesCommand.parse_obj(params)
        cmd()

        seq = SamplesSequence.from_underfolder(params["output"])
        assert len(seq) == expected_length

    @pytest.mark.parametrize("keys", ["image"])
    @pytest.mark.parametrize(
        "algorithm,raises",
        [
            ["blake2b", False],
            ["blake2s", False],
            ["md5", False],
            ["sha1", False],
            ["sha224", False],
            ["sha256", False],
            ["sha384", False],
            ["sha3_224", False],
            ["sha3_256", False],
            ["sha3_384", False],
            ["sha3_512", False],
            ["sha512", False],
            ["shake_128", True],
            ["shake_256", True],
            ["myalgorithm", True],
        ],
    )
    def test_filter_duplicates_algorithm(
        self, minimnist_dataset, keys, algorithm, raises, tmp_path
    ):
        from contextlib import nullcontext
        from pipelime.commands import FilterDuplicatesCommand

        context = pytest.raises(ValueError) if raises else nullcontext()
        with context:
            params = {
                "input": minimnist_dataset["path"].as_posix(),
                "output": (tmp_path / "output").as_posix(),
                "keys": keys,
                "algorithm": algorithm,
            }
            cmd = FilterDuplicatesCommand.parse_obj(params)
            cmd()
