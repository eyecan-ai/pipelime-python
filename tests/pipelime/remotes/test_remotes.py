import pytest
import pipelime.remotes as plr
import pipelime.sequences as pls

from pathlib import Path
from urllib.parse import ParseResult
from filecmp import cmp
import typing as t


class TestRemotes:
    def _upload_download(
        self,
        temp_folder,
        source: str,
        remote_url: ParseResult,
    ):
        from pipelime.remotes.base import BaseRemote

        remote = plr.create_remote(remote_url)
        assert isinstance(remote, BaseRemote)

        remote_base_path, _ = plr.paths_from_url(remote_url)
        assert remote_base_path is not None

        # upload
        source_to_remote: t.Mapping[Path, ParseResult] = {}
        reader = pls.SamplesSequence.from_underfolder(  # type: ignore
            source, merge_root_items=False
        )
        for sample in reader:
            for k, itm in sample.items():
                rm_url = remote.upload_file(itm._file_sources[0], remote_base_path)
                assert rm_url is not None
                source_to_remote[itm._file_sources[0]] = rm_url

        # download and compare
        local_root = temp_folder / "local"
        local_root.mkdir(parents=True)

        if remote_base_path.startswith("/"):
            remote_base_path = remote_base_path[1:]

        for original, rm_url in source_to_remote.items():
            rm, (rm_base_path, rm_name) = plr.create_remote(rm_url), plr.paths_from_url(
                rm_url
            )
            assert isinstance(rm, type(remote))
            assert isinstance(rm_base_path, str)
            assert isinstance(rm_name, str)
            assert rm_base_path == remote_base_path

            local_file = local_root / rm_name
            assert rm.download_file(local_file, rm_base_path, rm_name)
            assert cmp(str(local_file), original, shallow=False)

    def test_shared_folder_remote(self, minimnist_dataset: dict, tmp_path: Path):
        remote_url = plr.make_remote_url(
            scheme="file", host="localhost", bucket=(tmp_path / "rmbucket")
        )

        self._upload_download(tmp_path, minimnist_dataset["path"], remote_url)

    def test_s3_remote(self, minimnist_dataset: dict, tmp_path: Path, minio: str):
        if not minio:
            pytest.skip("MinIO unavailable")

        remote_url = plr.make_remote_url(
            scheme="s3",
            user=f"{minio}",
            password=f"{minio}",
            host="localhost",
            port=9000,
            bucket="rmbucket",
            secure=False,
        )

        self._upload_download(tmp_path, minimnist_dataset["path"], remote_url)
