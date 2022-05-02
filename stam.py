date = '2022-01-01'
accounts = [1, 2, 3]


def campaign_partition_values_fn():
    """
    generate partition values for the GlueBatchCreatePartitionsOperator
    """
    values = partition_values_fn()
    hours = range(0, 23)
    return [dict(Values=[value['Values'][0], value['Values'][1], str(hour)]) for value in values for hour in hours]


def partition_values_fn():
    """
    generate partition values for the GlueBatchCreatePartitionsOperator
    """
    return [dict(Values=[acc, date]) for acc in accounts]


if __name__ == "__main__":
    result = partition_values_fn()
    print(result)
    result = campaign_partition_values_fn()
    print(result)
