import boto3
from typing import List


class S3ListKeysOperator:
    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def get_conn():
        return boto3.client("s3", region_name="us-west-2")

    def list_keys(
        self, bucket_name, prefix="", delimiter="", page_size=None, max_items=None
    ):
        """
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        """
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        response = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig=config,
        )

        has_results = False
        keys = []
        for page in response:
            if "Contents" in page:
                has_results = True
                for k in page["Contents"]:
                    keys.append(k["Key"])

        if has_results:
            return keys

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

            keys_found = self.list_keys(
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
