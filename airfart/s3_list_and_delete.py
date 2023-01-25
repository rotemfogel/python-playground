import hashlib
import json
import os
from datetime import timedelta
from typing import Callable, List, Dict, Iterable, Optional

import pendulum
from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.glue_catalog import AwsGlueCatalogHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv
from pendulum import DateTime


class S3ListAndDeleteOperator(LoggingMixin):
    """
    List all objects from the bucket with the given string prefix in name.

    This operator returns a python list with the name of objects which can be
    used by `xcom` in the downstream task.

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string to filters the objects whose name begin with
        such prefix. (templated)
    :type prefix: str
    :param delimiter: the delimiter marks key hierarchy. (templated)
    :type delimiter: str
    :param recursive: Whether to get the list of objects to delete
     by recursive traversal for the provided prefix
    :type recursive: bool
    :param retention: If provided, delete S3 objects outside this retention period.
        Only fields `days` and `weeks` are considered.
    :type retention: timedelta
    :param retention_fn: If provided, set retention period using this function.
    :type retention_fn: Callable[[timedelta, str], timedelta]
    :param database_to_check: If provided, make sure that deletion of S3 keys
        won't cause the table `table_to_check` in the database `database_to_check`
        loosing (some) data in all S3 prefixes under the table's location.
    :type database_to_check: str
    :param table_to_check: If provided, make sure that deletion of S3 keys
        won't cause the table `table_to_check` in the database `database_to_check`
        loosing (some) data in all S3 prefixes under the table's location.
    :type table_to_check: str
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
        Default is set to 5 minutes. (based on max of 1m:2s, of 309323 runs)
    :type execution_timeout: datetime.timedelta
    :param aws_conn_id: The connection ID to use when connecting to S3 storage.
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str


    **Example**:
        The following operator would list all the files
        (excluding subfolders) from the S3
        ``customers/2018/04/`` key in the ``data`` bucket. ::

            s3_file = S3ListOperator(
                task_id='list_3s_files',
                bucket='data',
                prefix='customers/2018/04/',
                delimiter='/',
                aws_conn_id='aws_customers_conn'
            )
    """

    template_fields = (
        "bucket",
        "prefix",
        "delimiter",
        "retention",
        "retention_fn",
        "database_to_check",
        "table_to_check",
    )
    ui_color = "#ffd700"

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        prefix_fn: Callable[[Dict], List[str]] = None,
        retention: timedelta = None,
        retention_fn: Callable[[timedelta, str], timedelta] = None,
        delimiter: str = "",
        database_to_check: str = None,
        table_to_check: str = None,
        *args,
        **kwargs,
    ):
        super(S3ListAndDeleteOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.prefix_fn = prefix_fn
        self.delimiter = delimiter
        self.retention = retention
        self.retention_fn = retention_fn
        self.database_to_check = database_to_check
        self.table_to_check = table_to_check
        self.hook = S3Hook()

    @staticmethod
    def __keys_to_cleanup(keys, cleanup_up_to_date):
        result = []
        for key in keys:
            if "date" in key:
                if "$folder$" in key:
                    result.append(key)
                else:
                    d = key.split("date_")[-1].split("/")[0].split("=")[-1]
                    if "T" in d:
                        d = d.split("T")[0]
                    else:
                        if d.count("-") > 2:  # if date pattern is YYYY-MM-DD-HH24-MI
                            d = "-".join(d.split("-")[:3])
                    key_date = pendulum.parse(d, tz="America/New_York")
                    if key_date <= cleanup_up_to_date:
                        result.append(key)
        return result

    @staticmethod
    def __table_location(database, table):
        glue_hook = AwsGlueCatalogHook()
        response = glue_hook.get_table(database, table)
        table_location = response["StorageDescriptor"]["Location"]
        return table_location

    def __s3_prefixes_within_location(self, location, hook):
        components = location.split("/")
        bucket = components[2]
        prefix = "/".join(components[3:])
        keys = hook.list_keys_in_prefix_list(bucket, [prefix], self.delimiter)
        prefixes = {"/".join(key.split("/")[:-1]) for key in keys}
        return prefixes

    def chunk_delete_objects(self, bucket, keys, n=1000):
        key_chunks = [
            keys[i * n : (i + 1) * n] for i in range((len(keys) + n - 1) // n)
        ]
        for chunk in key_chunks:
            self.delete_objects(bucket=bucket, keys=chunk)

    def get_s3_file_schema_list_columns(self, s3_file_path, bucket) -> list:
        s3_file_schema = self.read_key(key=s3_file_path, bucket_name=bucket)
        type_renaming = {"integer": "int", "long": "bigint", "byte": "tinyint"}
        list_of_dict = [
            json.loads(elem) for elem in s3_file_schema.split("\n") if len(elem) > 0
        ]
        return [
            {
                "row_num": elem.get("row_num"),
                "Name": elem.get("name"),
                "Type": type_renaming.get(elem.get("type"), elem.get("type")),
            }
            for elem in list_of_dict
        ]

    def get_s3_file_path(self, bucket: str, prefix: str) -> Optional[str]:
        keys = self.hook.list_keys(bucket_name=bucket, prefix=prefix)
        if prefix in keys:
            keys.remove(prefix)
        if not keys:
            self.log.info(
                "No files in {bucket}/{prefix}".format(bucket=bucket, prefix=prefix)
            )
            return None
        elif len(keys) > 1:
            raise AirflowException(
                "\n\nBucket: %s at prefix: %s has more then 1 file.", bucket, prefix
            )
        else:
            return keys[0]

    def list_keys_in_prefix_list(
        self, bucket: str, prefix_list: List[str], delimiter: str = ""
    ) -> List[str]:
        keys: List[str] = []
        assert prefix_list is not None
        for prefix in prefix_list:
            self.log.info(
                "Listing files from bucket: %s in prefix: %s (Delimiter {%s)",
                bucket,
                prefix,
                delimiter,
            )

            keys_found = self.hook.list_keys(
                bucket_name=bucket,
                prefix=prefix,
                delimiter=delimiter,
            )

            if keys_found:
                self.log.debug(
                    "Following files listed for deletion:\n %s\nbucket: %s prefix: %s (Delimiter {%s))",
                    keys_found,
                    bucket,
                    prefix,
                    delimiter,
                )
                keys.extend(keys_found)
            else:
                self.log.info(
                    "No files in bucket: %s in prefix: %s (Delimiter {%s)",
                    bucket,
                    prefix,
                    delimiter,
                )

        return keys

    def copy_keys(
        self,
        source_bucket: str,
        source_keys: List[str],
        dest_bucket: str,
        dest_prefix: str,
    ) -> None:
        assert source_keys is not None
        for key in source_keys:
            key_split: List[str] = key.rsplit("/", 1)
            filename: str = key_split[-1]
            filename_prefix: str = hashlib.md5(key_split[0].encode("UTF-8")).hexdigest()
            new_filename: str = "-".join([filename_prefix, filename])
            dest_key: str = "/".join([dest_prefix, new_filename])
            self.copy_object(key, dest_key, source_bucket, dest_bucket)

    def get_prefixes_size(self, prefixes: Iterable[list], bucket: str):
        size = 0
        paginator = self.hook.get_conn().get_paginator("list_objects_v2")
        for prefix in prefixes:
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            for page in pages:
                if "Contents" in page:
                    for key in page["Contents"]:
                        x = key["Size"] / 1024 / 1024
                        size += x
        return size

    def validate_keys(
        self, bucket: str, prefix: str, file_suffix: str = "parquet"
    ) -> bool:
        keys = self.hook.list_keys(bucket_name=bucket, prefix=prefix)
        if keys:
            filtered_keys = list(filter(lambda k: file_suffix in k, keys))
            return len(filtered_keys) > 0
        return False

    def execute(self, context):
        prefix_list: List[str] = (
            self.prefix_fn(context) if self.prefix_fn else [self.prefix]
        )

        keys: List[str] = self.list_keys_in_prefix_list(
            self.bucket, prefix_list, self.delimiter
        )

        if self.retention_fn:
            self.retention = self.retention_fn(self.retention, self.dag_id)

        if self.retention:
            self.log.info(f"Retention is {self.retention.days} days")
            execution_date = context["execution_date"]
            cleanup_up_to_date = execution_date.subtract(days=self.retention.days)
            keys = self.__keys_to_cleanup(keys, cleanup_up_to_date)

        if len(keys) == 0:
            self.log.info(
                f'No files in bucket: {self.bucket}\n prefix: {prefix_list} (Delimiter: "{self.delimiter}")'
            )
            return

        if self.database_to_check and self.table_to_check:
            table_location = self.__table_location(
                self.database_to_check, self.table_to_check
            )
            table_prefixes = self.__s3_prefixes_within_location(table_location, hook)
            prefixes_to_delete = {"/".join(key.split("/")[:-1]) for key in keys}
            intact_prefixes = table_prefixes - prefixes_to_delete
            if intact_prefixes:
                self.log.info(
                    f"The following S3 prefixes are intact after the purge process: {intact_prefixes}"
                )
                hook.chunk_delete_objects(bucket=self.bucket, keys=keys)
        else:
            hook.chunk_delete_objects(bucket=self.bucket, keys=keys)


if __name__ == "__main__":
    load_dotenv()
    context = {"execution_date": DateTime(2022, 2, 23, 15, 29, 16)}
    bucket = os.getenv("DATA_BUCKET")
    db = os.getenv("DATA_DB")
    table = os.getenv("DATA_TABLE")
    s3_list_and_delete = S3ListAndDeleteOperator(bucket=bucket, prefix=f"{db}/{table}")
    s3_list_and_delete.execute(context)
