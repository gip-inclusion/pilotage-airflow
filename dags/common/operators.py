import pathlib
from collections.abc import Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context


class S3SyncLocalFilesystem(BaseOperator):
    template_fields: Sequence[str] = ("directory", "dest_key_prefix", "dest_bucket")

    def __init__(
        self,
        *,
        directory: str,
        dest_key_prefix: str = "",
        dest_bucket: str | None = None,
        aws_conn_id: str | None = "aws_default",
        verify: str | bool | None = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.directory = pathlib.Path(directory)
        self.dest_key_prefix = dest_key_prefix
        self.dest_bucket = dest_bucket
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def _files_to_copy(self):
        for dirpath, _, filenames in self.directory.walk(follow_symlinks=True):
            yield from [(dirpath / name).absolute() for name in filenames]

    def execute(self, context: Context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        for filename in self._files_to_copy():
            s3_bucket, s3_key = s3_hook.get_s3_bucket_key(
                self.dest_bucket,
                f"{self.dest_key_prefix}{filename.relative_to(self.directory.absolute())}",
                "dest_bucket",
                "dest_key",
            )
            self.log.info(f"Copying {filename} to s3://{s3_bucket}/{s3_key}")
            s3_hook.load_file(
                filename,
                s3_key,
                s3_bucket,
                self.replace,
                self.encrypt,
                self.gzip,
                self.acl_policy,
            )
