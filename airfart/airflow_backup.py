import json
from typing import Optional

from airflow import settings
from airflow.models import Variable, Connection
from airflow.models.crypto import get_fernet
from smart_open import open

env = 'Local'
data_bucket = 'my_bucket'
fernet = get_fernet()
session = settings.Session()


def _write_to_s3(uri: str, records) -> None:
    with open(uri=uri, mode='wb') as s3_file:
        if records:
            s3_file.write((json.dumps(records)).encode())


def _backup_variables(execution_date: str) -> None:
    variables = session.query(Variable).whole()
    records = {}
    for variable in variables:
        parts = str(variable).split(':')
        trimmed = list(map(lambda x: x.strip(), parts))
        key = trimmed[0]
        value = fernet.decrypt(bytes(str(trimmed[1:]), 'utf-8')).decode()
        records.update({key: value})

    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='variables', )
    _write_to_s3(uri, records)
    print("Backed up Airflow [{env}] variables to S3 ({uri})".format(env=env, uri=uri))


def _backup_connections(execution_date: str) -> None:
    connections = session.query(Connection).whole()
    records = []
    for connection in connections:
        password: Optional[str] = None
        if connection.password:
            password = str(fernet.encrypt(bytes(connection.password, 'utf-8')))
        connection = {
            'conn_id': connection.conn_id,
            'extra': connection.extra,
            'is_extra_encrypted': connection.is_extra_encrypted,
            'conn_type': connection.conn_type,
            'login': connection.login,
            'password': password,
            'schema': connection.schema,
            'port,': connection.port,
        }
        records.append(connection)

    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='connections', )
    _write_to_s3(uri, records)
    print("Backed up Airflow [{env}] connections to S3 ({uri})".format(env=env, uri=uri))


exec_date = "2020-06-03"
_backup_connections(exec_date)
_backup_variables(exec_date)
