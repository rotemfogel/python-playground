import threading
from random import randint
from random import seed

import boto3

namespace = 'Airflow'
env = 'staging'

client = boto3.Session(profile_name='sa-bi', region_name='us-west-2').client('cloudwatch')
seed(1)


def report_every_5_sec():
    threading.Timer(5.0, report_every_5_sec).start()
    response = client.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                'MetricName': 'heartbeat',
                'Dimensions': [
                    {
                        'Name': 'env',
                        'Value': env
                    },
                ],
                'Unit': 'None',
                'Value': randint(1, 5)
            },
        ]
    )
    print(response)


report_every_5_sec()
