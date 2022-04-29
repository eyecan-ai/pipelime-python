import pytest
from pathlib import Path
from urllib.parse import ParseResult
import typing as t

from pipelime.sequences import Sample, SamplesSequence
from pipelime.remotes import make_remote_url


class TestRemotes:
    def _normalized_url(self, url: ParseResult) -> str:
        src_path = Path(url.path)
        if src_path.suffix:
            src_path = src_path.parent
        return ParseResult(
            scheme=url.scheme,
            netloc=url.netloc if url.netloc else "localhost",
            path=src_path.as_posix(),
            params="",
            query="",
            fragment="",
        ).geturl()

    def _upload_to_remote(  # noqa
        self,
        inpath: Path,
        outpath: Path,
        remote_urls: t.Union[ParseResult, t.Sequence[ParseResult]],
        filter_fn: t.Optional[t.Callable[[Sample], bool]],
        keys_to_upload: t.Optional[t.Collection[str]],
        check_data: bool,
    ):
        from pipelime.stages import StageUploadToRemote
        import pipelime.items as pli
        import numpy as np

        if isinstance(remote_urls, ParseResult):
            remote_urls = [remote_urls]

        filtered_seq = SamplesSequence.from_underfolder(  # type: ignore
            inpath, merge_root_items=False
        )
        if filter_fn:
            filtered_seq = filtered_seq.filter(filter_fn)

        out_seq = filtered_seq.map(
            StageUploadToRemote(remotes=remote_urls, keys_to_upload=keys_to_upload)
        ).to_underfolder(folder=outpath)
        for _ in out_seq:
            pass

        # high-level check
        if check_data:
            org_seq = SamplesSequence.from_underfolder(  # type: ignore
                inpath, merge_root_items=False
            )
            if filter_fn:
                org_seq = org_seq.filter(filter_fn)
            reader_out = SamplesSequence.from_underfolder(  # type: ignore
                outpath, merge_root_items=False
            )
            for org_sample, out_sample in zip(org_seq, reader_out):
                assert org_sample.keys() == out_sample.keys()

                for key, out_item in out_sample.items():
                    assert out_item._data_cache is None
                    if not keys_to_upload or key in keys_to_upload:
                        actual_remotes = set(
                            org_sample[key]._remote_sources + remote_urls
                        )

                        assert len(out_item._file_sources) == 0
                        assert len(out_item._remote_sources) == len(actual_remotes)
                        for rm_src in out_item._remote_sources:
                            for rm_trg in actual_remotes:
                                if self._normalized_url(rm_src) == self._normalized_url(
                                    rm_trg
                                ):
                                    break
                            else:
                                assert False
                    else:
                        assert len(out_item._file_sources) == 1
                        assert len(out_item._remote_sources) == 0

                for k, v in org_sample.items():
                    if isinstance(v, pli.NumpyItem):
                        assert np.array_equal(
                            v(), out_sample[k](), equal_nan=True  # type: ignore
                        )
                    else:
                        assert v() == out_sample[k]()

    def test_shared_folder_upload(
        self, minimnist_private_dataset: dict, tmp_path: Path
    ):
        # data lake
        remote_url = make_remote_url(
            scheme="file",
            netloc="localhost",
            path=(tmp_path / "rmbucket"),
        )

        self._upload_to_remote(
            minimnist_private_dataset["path"],
            tmp_path / "output",
            remote_url,
            None,
            None,
            True,
        )

    def test_s3_upload(
        self, minimnist_private_dataset: dict, tmp_path: Path, minio: str
    ):
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

        self._upload_to_remote(
            minimnist_private_dataset["path"],
            tmp_path / "output",
            remote_url,
            None,
            None,
            True,
        )

    def test_incremental_file_upload(
        self, minimnist_private_dataset: dict, tmp_path: Path
    ):
        import pipelime.items as pli
        from shutil import rmtree
        import numpy as np

        # data lake
        remote_root = tmp_path / "file_remote"
        remote_root.mkdir()
        remote_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_root / "rmbucket"
        )

        # upload even samples
        even_output = tmp_path / "even_uploaded"
        self._upload_to_remote(
            minimnist_private_dataset["path"],
            even_output,
            remote_url,
            lambda x: int(x["label"]()) % 2 == 0,
            minimnist_private_dataset["image_keys"],
            True,
        )

        # manually merge odd and even samples
        even_odd_partial_output = tmp_path / "even_odd_partial"

        even_seq = SamplesSequence.from_underfolder(  # type: ignore
            even_output, merge_root_items=False
        )
        even_count = len(even_seq)
        assert even_count == minimnist_private_dataset["len"] // 2

        even_odd_partial_seq = even_seq.cat(
            SamplesSequence.from_underfolder(  # type: ignore
                minimnist_private_dataset["path"], merge_root_items=False
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
            minimnist_private_dataset["image_keys"],
            False,
        )

        # only the odd samples are on the remote
        even_odd_reader = SamplesSequence.from_underfolder(  # type: ignore
            even_odd_output, merge_root_items=False
        )
        assert len(even_odd_reader) == minimnist_private_dataset["len"]

        for s1, s2 in zip(even_odd_reader, even_odd_partial_seq):
            is_even = int(s1["label"]()) % 2 == 0
            assert s1.keys() == s2.keys()

            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__

                if (k in minimnist_private_dataset["image_keys"]) and is_even:
                    assert v1() is None
                    assert v2() is None
                    assert len(v1._file_sources) == 0
                    assert len(v1._remote_sources) == 1
                    assert len(v1._file_sources) == len(v2._file_sources)
                    assert len(v1._remote_sources) == len(v2._remote_sources)
                else:
                    assert v2() is not None
                    if k in minimnist_private_dataset["image_keys"]:
                        assert len(v1._file_sources) == 0
                        assert len(v1._remote_sources) == 1
                    else:
                        assert len(v1._file_sources) == 1
                        assert len(v1._remote_sources) == 0
                    # NB: when we called `to_underfolder` a new file source has been
                    # added (see above)
                    assert len(v2._file_sources) == 2
                    assert len(v2._remote_sources) == 0
                    if isinstance(v1, pli.NumpyItem):
                        assert np.array_equal(
                            v1(), v1(), equal_nan=True  # type: ignore
                        )
                    else:
                        assert v1() == v2()

    def test_multiple_remote_upload(
        self, minimnist_private_dataset: dict, tmp_path: Path
    ):
        from shutil import rmtree
        import numpy as np
        import pipelime.items as pli

        # create two remotes
        remote_a_root = tmp_path / "remote_a"
        remote_a_root.mkdir()
        remote_a_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_a_root / "rmbucketa"
        )

        remote_b_root = tmp_path / "remote_b"
        remote_b_root.mkdir()
        remote_b_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_b_root / "rmbucketb"
        )

        # upload to both remotes
        output_a_and_b = tmp_path / "output_a_and_b"
        self._upload_to_remote(
            minimnist_private_dataset["path"],
            output_a_and_b,
            [remote_a_url, remote_b_url],
            None,
            None,
            True,
        )

        # now remove remote_a and check again the data
        rmtree(remote_a_root, ignore_errors=True)

        input_seq = SamplesSequence.from_underfolder(  # type: ignore
            minimnist_private_dataset["path"], merge_root_items=False
        )
        output_seq = SamplesSequence.from_underfolder(output_a_and_b)  # type: ignore
        for ins, outs in zip(input_seq, output_seq):
            assert ins.keys() == outs.keys()
            for k, v in ins.items():
                if isinstance(v, pli.NumpyItem):
                    assert np.array_equal(
                        v(), outs[k](), equal_nan=True  # type: ignore
                    )
                else:
                    assert v() == outs[k]()

        # now upload to remote_c taking data from remote_b
        remote_c_root = tmp_path / "remote_c"
        remote_c_root.mkdir()
        remote_c_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_c_root / "rmbucketc"
        )

        output_c_from_b = tmp_path / "output_c_from_b"
        self._upload_to_remote(
            output_a_and_b,
            output_c_from_b,
            remote_c_url,
            None,
            None,
            True,
        )

    def test_forget_source(self, minimnist_private_dataset: dict, tmp_path: Path):
        from pipelime.stages import StageForgetSource
        import pipelime.items as pli
        import numpy as np

        # create two remotes
        remote_a_root = tmp_path / "remote_a"
        remote_a_root.mkdir()
        remote_a_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_a_root / "rmbucketa"
        )

        remote_b_root = tmp_path / "remote_b"
        remote_b_root.mkdir()
        remote_b_url = make_remote_url(
            scheme="file", netloc="localhost", path=remote_b_root / "rmbucketb"
        )

        # upload to both remotes
        key_noup_rm, key_noup_norm, key_up_rm = minimnist_private_dataset["item_keys"][
            0:3
        ]
        output_a_and_b = tmp_path / "output_a_and_b"
        self._upload_to_remote(
            minimnist_private_dataset["path"],
            output_a_and_b,
            [remote_a_url, remote_b_url],
            None,
            [
                k
                for k in minimnist_private_dataset["item_keys"]
                if k != key_noup_rm and k != key_noup_norm
            ],
            True,
        )

        # remove remote_a from all samples and remote_b from only one item key
        output_remove = tmp_path / "output_remove"
        seq = (
            SamplesSequence.from_underfolder(output_a_and_b)  # type: ignore
            .map(
                StageForgetSource(
                    remote_a_url,
                    **{
                        key_noup_rm: [output_a_and_b, remote_b_url],
                        key_up_rm: remote_b_url,
                    },
                )
            )
            .to_underfolder(output_remove)
        )
        for _ in seq:
            pass

        # check final output
        normalized_a = self._normalized_url(remote_a_url)
        normalized_b = self._normalized_url(remote_b_url)
        org_seq = SamplesSequence.from_underfolder(output_a_and_b)  # type: ignore
        out_seq = SamplesSequence.from_underfolder(output_remove)  # type: ignore
        for org_sample, out_sample in zip(org_seq, out_seq):
            assert org_sample.keys() == out_sample.keys()
            for k, vout in out_sample.items():
                if k in (key_noup_rm, key_up_rm):
                    # this item should be a deep copy
                    assert len(vout._remote_sources) == 0
                    path = Path(vout._file_sources[0])
                    assert not path.is_symlink()
                    assert path.is_file()
                    assert path.stat().st_nlink == 1
                elif k == key_noup_norm:
                    # this item should be a hard link
                    assert len(vout._remote_sources) == 0
                    path = Path(vout._file_sources[0])
                    assert not path.is_symlink()
                    assert path.is_file()
                    assert path.stat().st_nlink == 3
                else:
                    norm_rm = [self._normalized_url(u) for u in vout._remote_sources]
                    assert normalized_a not in norm_rm
                    assert normalized_b in norm_rm
                    assert len(vout._file_sources) == 0

                if isinstance(vout, pli.NumpyItem):
                    assert np.array_equal(
                        vout(), org_sample[k](), equal_nan=True  # type: ignore
                    )
                else:
                    assert vout() == org_sample[k]()
