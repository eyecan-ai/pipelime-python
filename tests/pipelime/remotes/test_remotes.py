import pytest
import pipelime.remotes as plr
from pipelime.remotes.base import BaseRemote
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
        remote: BaseRemote,
        remote_base_path: str,
    ):
        # upload
        source_to_remote: t.Mapping[Path, ParseResult] = {}
        reader: pls.SamplesSequence = pls.SamplesSequence.from_underfolder(  # type: ignore
            source, merge_root_items=False
        )
        for sample in reader:
            for k, itm in sample.items():
                remote_url = remote.upload_file(itm._file_sources[0], remote_base_path)
                assert remote_url is not None
                source_to_remote[itm._file_sources[0]] = remote_url

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
        remote = plr.create_remote(
            ParseResult(
                scheme="file",
                netloc="localhost",
                path="",
                params="",
                query="",
                fragment="",
            )
        )
        assert isinstance(remote, BaseRemote)

        remote_root = tmp_path / "rm_bucket"
        remote_root.mkdir(parents=True)
        remote_root = remote_root.as_posix()

        self._upload_download(tmp_path, minimnist_dataset["path"], remote, remote_root)

    def test_s3_remote(self, minimnist_dataset: dict, tmp_path: Path, minio):
        if not minio:
            pytest.skip("MinIO unavailable")

        s3_init_args = {
            "access_key": f"{minio}",
            "secret_key": f"{minio}",
            "secure_connection": False,
        }

        remote = plr.create_remote(
            ParseResult(
                scheme="s3",
                netloc="localhost:9000",
                path="",
                params="",
                query=":".join([k + "=" + str(v) for k, v in s3_init_args.items()]),
                fragment="",
            )
        )
        assert isinstance(remote, BaseRemote)
        self._upload_download(tmp_path, minimnist_dataset["path"], remote, "rmbucket")
