import pytest

from .test_general_base import TestGeneralCommandsBase


class TestStageTiming(TestGeneralCommandsBase):
    @pytest.mark.parametrize("skip_first", [0, 1, 5])
    @pytest.mark.parametrize("max_samples", [1, 10, None])
    @pytest.mark.parametrize("repeat", [1, 3])
    @pytest.mark.parametrize("process_timer", [True, False])
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 8])
    def test_stage_timing(
        self,
        minimnist_dataset,
        skip_first,
        max_samples,
        repeat,
        process_timer,
        nproc,
        prefetch,
    ):
        from pipelime.commands import StageTimingCommand
        from pipelime.stages import StageCropAndPad, StageDuplicateKey, StageSampleHash

        params = {
            "stages": [
                StageCropAndPad(x=-10, y=-10, images="image"),  # type: ignore
                StageDuplicateKey(source_key="image", copy_to="image2"),  # type: ignore
                StageCropAndPad(x=-10, y=-10, images="mask"),  # type: ignore
                StageSampleHash(keys=["image", "image2", "mask", "points", "label"]),  # type: ignore
            ],
            "input": minimnist_dataset["path"].as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "skip_first": skip_first,
            "max_samples": max_samples,
            "repeat": repeat,
            "process": process_timer,
        }
        cmd = StageTimingCommand.parse_obj(params)
        cmd()

        # check output
        assert cmd.average_time is not None

        if (max_samples is not None and skip_first >= max_samples) or (
            nproc > 0 and skip_first >= prefetch
        ):
            assert cmd.average_time.stages == {}
        else:
            assert {
                "crop-and-pad-images-1",
                "crop-and-pad-images-2",
                "duplicate-key-1",
                "sample-hash-1",
            } == set(cmd.average_time.stages.keys())

        for v in cmd.average_time.stages.values():
            assert v.nanosec >= 0
