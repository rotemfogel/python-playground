import boto3

_glue_hook = boto3.Session(profile_name='default', region_name='us-west-2').client('glue')
chunk_size = 100
_dry_run = True


def _get(operation: str,
         page_key: str,
         value_key: str,
         **kwargs) -> list:
    paginator = _glue_hook.get_paginator(operation)
    response = paginator.paginate(**kwargs)
    results = list()
    for page in response:
        for key in page[page_key]:
            if value_key:
                value = key[value_key]
                if type(value) == list:
                    results.extend(key[value_key])
                else:
                    results.append(key[value_key])
            else:
                results.append(key)
    return results


def _get_database(db: str) -> dict:
    try:
        args = {'Name': db}
        return _glue_hook.get_database(**args)
    except Exception as e:
        if 'EntityNotFoundException' in str(e):
            return None
        raise e


def _get_databases() -> list:
    return _get('get_databases', 'DatabaseList', 'Name')


def _get_tables(db: str) -> list:
    args = {'DatabaseName': db}
    return _get('get_tables', 'TableList', 'Name', **args)


def _get_partitions(db: str, table: str) -> list:
    args = {
        'DatabaseName': db,
        'TableName': table
    }
    return _get('get_partitions', 'Partitions', value_key=None, **args)


def _get_versions(db: str, table: str) -> list:
    args = {
        'DatabaseName': db,
        'TableName': table
    }
    return _get('get_table_versions', 'TableVersions', 'VersionId', **args)


def _clear_table(db: str, table: str) -> None:
    # partitions = _get_partitions(db, table)
    # print(f'{db}.{table} has {len(partitions)} partitions')
    versions = sorted(list(map(lambda x: int(x), _get_versions(db, table))))
    if len(versions) > chunk_size:
        versions_to_drop = versions[:-chunk_size]
        version_chunks = [versions_to_drop[i * chunk_size:(i + 1) * chunk_size] for i in
                          range((len(versions_to_drop) + chunk_size - 1) // chunk_size)]
        for chunk in version_chunks:
            if not _dry_run:
                _glue_hook.batch_delete_table_version(
                    DatabaseName=db,
                    TableName=table,
                    VersionIds=list(map(lambda x: str(x), chunk))
                )
                print(f'dropped {len(versions_to_drop)} versions out of {len(versions)} from table {db}.{table}')
            else:
                print(f'skipping drop due to dryRun: {db}.{table} -> {chunk}')


def _clear_tables(db: str) -> None:
    tables = list(filter(lambda x: not str(x).startswith('tmp_'), _get_tables(db)))
    for table in tables:
        _clear_table(db, table)


def execute():
    databases = sorted(_get_databases())
    for db in databases:
        if _get_database(db):
            _clear_tables(db)


if __name__ == '__main__':
    execute()
