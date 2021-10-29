import boto3


class BaseAwsHook:
    def __init__(self,
                 client_type: str) -> None:
        super(BaseAwsHook, self).__init__()
        self.client_type = client_type

    def get_conn(self):
        return boto3.Session(profile_name='default', region_name='us-west-2').client(self.client_type)
