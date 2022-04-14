import pytest
from pathlib import Path
from urllib.parse import ParseResult
import typing as t

from pipelime.sequences import Sample, SamplesSequence
from pipelime.remotes import make_remote_url


class TestRemotes:
    def _upload_to_remote(
        self,
        dataset: dict,
        outpath: Path,
        remote_url: ParseResult,
        filter_fn: t.Optional[t.Callable[[Sample], bool]],
        check_data: bool,
    ):
        from pipelime.stages import StageUploadToRemote
        from pipelime.items.numpy_item import NumpyItem
        import numpy as np

        filtered_seq = SamplesSequence.from_underfolder(  # type: ignore
            dataset["path"], merge_root_items=False
        )
        if filter_fn:
            filtered_seq = filtered_seq.filter(filter_fn)
        out_seq = filtered_seq.map(StageUploadToRemote(remote_url)).to_underfolder(
            folder=outpath
        )
        for _ in out_seq:
            pass

        # high-level check
        if check_data:
            reader_out = SamplesSequence.from_underfolder(  # type: ignore
                outpath, merge_root_items=False
            )
            for org_sample, out_sample in zip(filtered_seq, reader_out):
                for out_item in out_sample.values():
                    print(out_item)
                    assert out_item._data_cache is None
                    assert len(out_item._file_sources) == 0
                    assert len(out_item._remote_sources) == 1

                assert org_sample.keys() == out_sample.keys()
                for k, v in org_sample.items():
                    if isinstance(v, NumpyItem):
                        assert np.array_equal(v(), out_sample[k](), equal_nan=True)  # type: ignore
                    else:
                        assert v() == out_sample[k]()

    def test_shared_folder_upload(self, minimnist_dataset: dict, tmp_path: Path):
        # data lake
        remote_url = make_remote_url(
            scheme="file",
            netloc="localhost",
            path="/" + tmp_path.as_posix() + "/rmbucket",
        )

        self._upload_to_remote(
            minimnist_dataset, tmp_path / "output", remote_url, None, True
        )

    def test_s3_upload(self, minimnist_dataset: dict, tmp_path: Path, minio: str):
        # data lake
        if not minio:
            pytest.skip("MinIO unavailable")

        remote_url = make_remote_url(
            scheme="s3",
            netloc="localhost:9000",
            path="/rmbucket",
            access_key=f"{minio}",
            secret_key=f"{minio}",
            secure_connection=False,
        )

        self._upload_to_remote(
            minimnist_dataset, tmp_path / "output", remote_url, None, True
        )
