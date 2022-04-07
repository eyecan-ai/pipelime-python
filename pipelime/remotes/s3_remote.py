from pipelime.remotes.base import BaseRemote
from loguru import logger
import hashlib

import typing as t


class S3Remote(BaseRemote):
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "sha256"

    def __init__(
        self,
        endpoint: str,
        access_key: t.Optional[str] = None,
        secret_key: t.Optional[str] = None,
        session_token: t.Optional[str] = None,
        secure_connection: bool = True,
        region: t.Optional[str] = None,
    ):
        """S3-compatible remote. Credential can be passed or retrieved from:
            * env var:
                ** access key: AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, MINIO_ACCESS_KEY
                ** secret key: AWS_SECRET_ACCESS_KEY, AWS_SECRET_KEY, MINIO_SECRET_KEY
                ** session token: AWS_SESSION_TOKEN
            * config files:
                ** ~/.aws/credentials
                ** ~/[.]mc/config.json

        :param endpoint: the endpoint address
        :type endpoint: str
        :param access_key: optional user id, defaults to None
        :type access_key: Optional[str], optional
        :param secret_key: optional user password, defaults to None
        :type secret_key: Optional[str], optional
        :param session_token: optional session token, defaults to None
        :type session_token: Optional[str], optional
        :param secure_connection: should use a secure connection, defaults to True
        :type secure_connection: bool, optional
        :param region: optional region, defaults to None
        :type region: Optional[str], optional
        """
        super().__init__(endpoint)

        try:
            from minio import Minio

            from minio.credentials import (
                ChainedProvider,
                EnvAWSProvider,
                EnvMinioProvider,
                AWSConfigProvider,
                MinioClientConfigProvider,
            )

            self._client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                secure=secure_connection,
                region=region,
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

    @property
    def client(self):
        return self._client

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
            except Exception as exc:
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
            except Exception as exc:
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
            except Exception as exc:
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
            except Exception as exc:
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
