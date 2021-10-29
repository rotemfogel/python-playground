from airfart.base_aws_hook import BaseAwsHook


class AwsS3Hook(BaseAwsHook):

    def __init__(self) -> None:
        super().__init__('s3')

    def validate_key(self,
                     bucket: str,
                     prefix: str) -> bool:
        keys = self.get_conn().list_objects(
            Bucket=bucket,
            MaxKeys=10,
            Prefix=prefix
        )
        if keys.get('Contents'):
            filtered_keys = list(filter(lambda k: 'parquet' in k['Key'], keys['Contents']))
            return len(filtered_keys) > 0
        return False
