import pytest
from pathlib import Path
from urllib.parse import ParseResult
import typing as t

from pipelime.sequences import Sample, SamplesSequence
from pipelime.remotes import make_remote_url


class TestRemotes:
    def _get_local_copy(self, dataset: dict, outpath: Path):
        from pipelime.items import item_serialization_mode, SerializationMode

        with item_serialization_mode(SerializationMode.DEEP_COPY):
            seq = SamplesSequence.from_underfolder(  # type: ignore
                dataset["path"], merge_root_items=False
            ).to_underfolder(folder=outpath)
            for _ in seq:
                pass

    def _upload_to_remote(
        self,
        inpath: Path,
        outpath: Path,
        remote_url: ParseResult,
        filter_fn: t.Optional[t.Callable[[Sample], bool]],
        keys_to_upload: t.Optional[t.Collection[str]],
        check_data: bool,
    ):
        from pipelime.stages import StageUploadToRemote
        from pipelime.items.numpy_item import NumpyItem
        import numpy as np

        filtered_seq = SamplesSequence.from_underfolder(  # type: ignore
            inpath, merge_root_items=False
        )
        if filter_fn:
            filtered_seq = filtered_seq.filter(filter_fn)
        out_seq = filtered_seq.map(
            StageUploadToRemote(remote_url, keys_to_upload=keys_to_upload)
        ).to_underfolder(folder=outpath)
        for _ in out_seq:
            pass

        # high-level check
        if check_data:
            reader_out = SamplesSequence.from_underfolder(  # type: ignore
                outpath, merge_root_items=False
            )
            for org_sample, out_sample in zip(filtered_seq, reader_out):
                for key, out_item in out_sample.items():
                    assert out_item._data_cache is None
                    if not keys_to_upload or key in keys_to_upload:
                        assert len(out_item._file_sources) == 0
                        assert len(out_item._remote_sources) == 1
                    else:
                        assert len(out_item._file_sources) == 1
                        assert len(out_item._remote_sources) == 0

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
            path=(tmp_path / "rmbucket"),
        )

        self._get_local_copy(minimnist_dataset, tmp_path / "input")
        self._upload_to_remote(
            tmp_path / "input", tmp_path / "output", remote_url, None, None, True
        )

    def test_s3_upload(self, minimnist_dataset: dict, tmp_path: Path, minio: str):
        # data lake
        if not minio:
            pytest.skip("MinIO unavailable")

        remote_url = make_remote_url(
            scheme="s3",
            netloc="localhost:9000",
            path="rmbucket",
            access_key=f"{minio}",
            secret_key=f"{minio}",
            secure_connection=False,
        )

        self._get_local_copy(minimnist_dataset, tmp_path / "input")
        self._upload_to_remote(
            tmp_path / "input", tmp_path / "output", remote_url, None, None, True
        )

    def test_incremental_file_upload(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli
        from pipelime.items.numpy_item import NumpyItem
        from shutil import rmtree
        import numpy as np

        # data lake
        remote_root = tmp_path / "file_remote"
        remote_root.mkdir()
        remote_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_root / "rmbucket"
        )

        self._get_local_copy(minimnist_dataset, tmp_path / "input")

        # upload even samples
        even_output = tmp_path / "even_uploaded"
        self._upload_to_remote(
            tmp_path / "input",
            even_output,
            remote_url,
            lambda x: int(x["label"]()) % 2 == 0,
            minimnist_dataset["image_keys"],
            True,
        )

        # manually merge odd and even samples
        even_odd_partial_output = tmp_path / "even_odd_partial"

        even_seq = SamplesSequence.from_underfolder(  # type: ignore
            even_output, merge_root_items=False
        )
        even_count = len(even_seq)
        assert even_count == minimnist_dataset["len"] // 2

        even_odd_partial_seq = even_seq.cat(
            SamplesSequence.from_underfolder(  # type: ignore
                tmp_path / "input", merge_root_items=False
            ).filter(lambda x: int(x["label"]()) % 2 == 1)
        )

        # NB: `to_underfolder` generates a new sequence, but Items are shallow-copied.
        # When serializing to disk w/o uploading to remote, the original items are
        # updated with the new sources, since the operation is inherently safe.
        # Therefore, items in `even_odd_partial_seq` will have 2 file sources.
        for _ in even_odd_partial_seq.to_underfolder(folder=even_odd_partial_output):
            pass

        # clear all remote data
        rmtree(remote_root, ignore_errors=True)
        remote_root.mkdir()

        # upload all samples, but only the odd ones are actually copied to the remote
        even_odd_output = tmp_path / "even_odd_uploaded"
        self._upload_to_remote(
            even_odd_partial_output,
            even_odd_output,
            remote_url,
            None,
            minimnist_dataset["image_keys"],
            False,
        )

        # only the odd samples are on the remote
        even_odd_reader = SamplesSequence.from_underfolder(  # type: ignore
            even_odd_output, merge_root_items=False
        )
        assert len(even_odd_reader) == minimnist_dataset["len"]

        for s1, s2 in zip(even_odd_reader, even_odd_partial_seq):
            is_even = int(s1["label"]()) % 2 == 0
            assert s1.keys() == s2.keys()

            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__

                if (k in minimnist_dataset["image_keys"]) and is_even:
                    assert v1() is None
                    assert v2() is None
                    assert len(v1._file_sources) == 0
                    assert len(v1._remote_sources) == 1
                    assert len(v1._file_sources) == len(v2._file_sources)
                    assert len(v1._remote_sources) == len(v2._remote_sources)
                else:
                    assert v2() is not None
                    if k in minimnist_dataset["image_keys"]:
                        assert len(v1._file_sources) == 0
                        assert len(v1._remote_sources) == 1
                    else:
                        assert len(v1._file_sources) == 1
                        assert len(v1._remote_sources) == 0
                    # NB: when we called `to_underfolder` a new file source has been
                    # added (see above)
                    assert len(v2._file_sources) == 2
                    assert len(v2._remote_sources) == 0
                    if isinstance(v1, NumpyItem):
                        assert np.array_equal(v1(), v1(), equal_nan=True)  # type: ignore
                    else:
                        assert v1() == v2()
