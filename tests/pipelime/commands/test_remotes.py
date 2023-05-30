from pathlib import Path

import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestRemotes(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize("keys", [None, ["metadata", "label"]])
    @pytest.mark.parametrize("slicing", [(None, None, None), (1, 5, 2), (-2, -7, -3)])
    def test_remotes(self, minimnist_dataset, nproc, prefetch, keys, slicing, tmp_path):
        from pipelime.commands import AddRemoteCommand, RemoveRemoteCommand
        from pipelime.remotes import make_remote_url
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
