import pytest
from pathlib import Path
import numpy as np
import pipelime.sequences as pls
import pipelime.items as pli


@pytest.mark.parametrize(
    ["num_workers", "keep_order", "prefetch"],
    [
        (0, False, 2),
        (1, False, 2),
        (4, False, 4),
        (4, True, 4),
        (-1, False, 4),
    ],
)
def test_grabber(
    num_workers: int,
    keep_order: bool,
    prefetch: int,
    minimnist_dataset: dict,
    tmp_path: Path,
):
    source = pls.SamplesSequence.from_underfolder(  # type: ignore
        folder=minimnist_dataset["path"], merge_root_items=True
    )
    proc = (
        source.slice(stop=10)
        .repeat(10)
        .enumerate(idx_key="ii")
        .to_underfolder(folder=tmp_path / "output")
    )
    grabber = pls.Grabber(
        num_workers=num_workers, keep_order=keep_order, prefetch=prefetch
    )

    with pli.item_serialization_mode(pli.SerializationMode.CREATE_NEW_FILE):
        itm_sm = pli.item_serialization_mode(
            pli.SerializationMode.CREATE_NEW_FILE,
        )
        pls.grab_all(grabber, proc, worker_init_fn=itm_sm.__enter__)

    for f in tmp_path.glob("output/**/*"):
        assert not f.is_symlink()
        assert f.stat().st_nlink == 1

    dest = pls.SamplesSequence.from_underfolder(  # type: ignore
        folder=tmp_path / "output", merge_root_items=True
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
