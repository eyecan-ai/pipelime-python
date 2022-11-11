import hashlib
import typing as t

from loguru import logger

from pipelime.remotes.base import BaseRemote, NetlocData


class S3Remote(BaseRemote):
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "blake2b"

    def __init__(self, netloc_data: NetlocData):
        """S3-compatible remote. Credential can be passed or retrieved from:
            * env var:
                ** access key: AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, MINIO_ACCESS_KEY
                ** secret key: AWS_SECRET_ACCESS_KEY, AWS_SECRET_KEY, MINIO_SECRET_KEY
                ** session token: AWS_SESSION_TOKEN
            * config files:
                ** ~/.aws/credentials
                ** ~/[.]mc/config.json

        Optional init arguments:
            * session (str): the session token (default: None)
            * secure (bool): establish a secure connection (default: True)
            * region (str): the preferred region (default: None)

        :param netloc_data: the network data info.
        :type netloc: NetlocData
        """
        super().__init__(netloc_data)

        try:
            from minio import Minio
            from minio.credentials import (
                AWSConfigProvider,
                ChainedProvider,
                EnvAWSProvider,
                EnvMinioProvider,
                MinioClientConfigProvider,
            )

            self._client = Minio(
                endpoint=netloc_data.host,
                access_key=netloc_data.user,
                secret_key=netloc_data.password,
                session_token=netloc_data.init_args.get("session", None),
                secure=netloc_data.init_args.get("secure", True),  # type: ignore
                region=netloc_data.init_args.get("region", None),
                credentials=ChainedProvider(
                    [
                        # AWS_ACCESS_KEY_ID|AWS_ACCESS_KEY,
                        # AWS_SECRET_ACCESS_KEY|AWS_SECRET_KEY,
                        # AWS_SESSION_TOKEN
                        EnvAWSProvider(),
                        # MINIO_ACCESS_KEY, MINIO_SECRET_KEY
                        EnvMinioProvider(),
                        # ~/.aws/credentials
                        AWSConfigProvider(),
                        # ~/[.]mc/config.json
                        MinioClientConfigProvider(),
                    ]
                ),
            )
        except ModuleNotFoundError:  # pragma: no cover
            logger.error("S3 remote needs `minio` python package.")
            self._client = None

    def _maybe_create_bucket(self, target_base_path: str):
        if not self._client.bucket_exists(target_base_path):  # type: ignore
            logger.info(
                f"Creating bucket '{target_base_path}' on S3 remote {self.netloc}."
            )
            self._client.make_bucket(target_base_path)  # type: ignore

    def _get_hash_fn(self, target_base_path: str) -> t.Any:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_base_path)
                tags = self._client.get_bucket_tags(target_base_path)  # type: ignore
                if tags is None:
                    from minio.commonconfig import Tags

                    tags = Tags()
                hash_fn_name = tags.get(self._HASH_FN_KEY_)

                # try-get
                if isinstance(hash_fn_name, str) and len(hash_fn_name) > 0:
                    try:
                        hash_fn = getattr(hashlib, hash_fn_name)
                        return hash_fn()
                    except AttributeError:
                        pass

                tags[self._HASH_FN_KEY_] = self._DEFAULT_HASH_FN_
                self._client.set_bucket_tags(target_base_path, tags)  # type: ignore

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:  # pragma: no cover
                logger.debug(str(exc))
        return None

    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        if self.is_valid:
            try:
                objlist = self._client.list_objects(  # type: ignore
                    target_base_path, prefix=target_name
                )
                _ = next(objlist)
                return True
            except StopIteration:
                return False
            except Exception as exc:  # pragma: no cover
                logger.debug(str(exc))
        return False

    def _upload(
        self,
        local_stream: t.BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                self._maybe_create_bucket(target_base_path)
                self._client.put_object(  # type: ignore
                    bucket_name=target_base_path,
                    object_name=target_name,
                    data=local_stream,
                    length=local_stream_size,
                )
                return True
            except Exception as exc:  # pragma: no cover
                logger.debug(str(exc))
                return False

        return False

    def _download(
        self,
        local_stream: t.BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            if not self._client.bucket_exists(source_base_path):  # type: ignore
                logger.debug(
                    f"Bucket '{source_base_path}' does not exist "
                    f"on S3 remote '{self.netloc}'."
                )
                return False

            response = None
            ok = False
            try:
                response = self._client.get_object(  # type: ignore
                    bucket_name=source_base_path,
                    object_name=source_name,
                    offset=source_offset,
                )
                for data in response.stream(amt=1024 * 1024):
                    local_stream.write(data)
                ok = True
            except Exception as exc:  # pragma: no cover
                logger.debug(str(exc))
                return False
            finally:
                if response:
                    response.close()
                    response.release_conn()
                return ok

        return False

    @classmethod
    def scheme(cls) -> str:
        return "s3"

    @property
    def is_valid(self) -> bool:
        return self._client is not None
