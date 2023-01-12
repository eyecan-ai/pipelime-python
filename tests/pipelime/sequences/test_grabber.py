import pytest
from pathlib import Path
import numpy as np
import pipelime.sequences as pls
import pipelime.items as pli


class TestGrabber:
    def _run_grabber(
        self,
        folder: Path,
        output: Path,
        num_workers: int,
        keep_order: bool,
        prefetch: int,
        sample_fn=None,
    ):
        from copy import deepcopy

        source = pls.SamplesSequence.from_underfolder(
            folder=folder, merge_root_items=True
        )
        proc = (
            source.slice(stop=10)
            .repeat(3)
            .enumerate(idx_key="ii")
            .to_underfolder(folder=output)
        )
        grabber = pls.Grabber(
            num_workers=num_workers, keep_order=keep_order, prefetch=prefetch
        )

        itm_sm = pli.item_serialization_mode(
            pli.SerializationMode.CREATE_NEW_FILE,
        )
        pls.grab_all(
            grabber,
            proc,
            grab_context_manager=deepcopy(itm_sm),
            worker_init_fn=itm_sm.__enter__,
            sample_fn=sample_fn,
        )

        for f in output.glob("**/*"):
            assert not f.is_symlink()
            assert not f.is_file() or f.stat().st_nlink == 1

        dest = pls.SamplesSequence.from_underfolder(
            folder=output, merge_root_items=True
        )

        for idx, dst_smpl in enumerate(dest):
            src_smpl = source[idx % 10]
            assert set(src_smpl.keys()) | {"ii"} == set(dst_smpl.keys())
            for k, v1 in src_smpl.items():
                v2 = dst_smpl[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, pli.NumpyItem):
                    assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
                else:
                    assert v1() == v2()

        return len(proc)

    @pytest.mark.parametrize(
        ["num_workers", "keep_order", "prefetch"],
        [
            (0, False, 2),
            (1, False, 2),
            (4, False, 4),
            (4, True, 4),
            (-1, False, 20),
        ],
    )
    def test_grabber(
        self,
        num_workers: int,
        keep_order: bool,
        prefetch: int,
        minimnist_dataset: dict,
        tmp_path: Path,
    ):
        self._run_grabber(
            minimnist_dataset["path"],
            tmp_path / "output_noreturn",
            num_workers,
            keep_order,
            prefetch,
        )

        counter = 0

        def _counter_fn(x):
            nonlocal counter
            counter += 1

        total_count = self._run_grabber(
            minimnist_dataset["path"],
            tmp_path / "output_sample",
            num_workers,
            keep_order,
            prefetch,
            _counter_fn,
        )
        assert counter == total_count

        counter = 0

        def _iota_fn(x, idx):
            nonlocal counter
            counter += idx

        total_count = self._run_grabber(
            minimnist_dataset["path"],
            tmp_path / "output_sample_idx",
            num_workers,
            keep_order,
            prefetch,
            _iota_fn,
        )
        assert counter == sum(range(total_count))
