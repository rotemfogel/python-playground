import boto3


def fetch(nt=None):
    if next_token is not None:
        res = client.get_tables(
            DatabaseName='dbl',
            NextToken=nt,
            MaxResults=100
        )
    else:
        res = client.get_tables(
            DatabaseName='dbl',
            MaxResults=100
        )
    return res


client = boto3.Session(profile_name='sa-bi', region_name='us-west-2').client('glue')
next_token = None
run = True
while run:
    response = fetch(next_token)
    print(response)
    tables = list(map(lambda x: x['Name'], response['TableList']))
    print(tables)
    filtered = list(filter(lambda x: x.startswith('dw'), tables))
    print(filtered)
    if not filtered:
        run = False
        break
    delete_response = client.batch_delete_table(
        DatabaseName='dbl',
        TablesToDelete=filtered
    )
    print(delete_response['Errors'])
    next_token = response['NextToken']

#
# response = client.batch_delete_table(
#     Namespace=namespace,
#     MetricData=[
#         {
#             'MetricName': 'heartbeat',
#             'Dimensions': [
#                 {
#                     'Name': 'env',
#                     'Value': env
#                 },
#             ],
#             'Unit': 'None',
#             'Value': randint(1, 5)
#         },
#     ]
# )
# print(response)
