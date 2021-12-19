import boto3

from airfart.base_hook import BaseHook


class BaseAwsHook(BaseHook):
    def __init__(self,
                 conn_id: str,
                 client_type: str) -> None:
        super(BaseAwsHook, self).__init__(conn_id=conn_id)
        self.client_type = client_type

    def get_conn(self):
        return boto3.Session(profile_name='default', region_name='us-west-2').client(self.client_type)
