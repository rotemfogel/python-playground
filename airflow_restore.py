import json

from airflow import settings
from airflow.models import Variable, Connection
from airflow.models.crypto import get_fernet
from smart_open import open

env = 'Local'
data_bucket = 'my_bucket'
fernet = get_fernet()
session = settings.Session()


def _read_from_s3(uri: str) -> list:
    records = []
    with open(uri=uri, mode='r') as s3_file:
        for line in s3_file:
            records.append(str(line))
    return records


def _restore_variables() -> None:
    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='variables', )

    raw_variables = _read_from_s3(uri)
    variables = json.loads(''.join(raw_variables))
    for k, v in variables.items():
        if not Variable.get(k):
            Variable.set(k, v)

    print("Restored Airflow [{env}] {len} variables from S3 ({uri})".format(env=env, len=len(variables), uri=uri))


def _restore_connections() -> None:
    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='connections', )
    raw_connections = _read_from_s3(uri)
    connections = json.loads(''.join(raw_connections))

    restored: int = 0
    for connection in connections:
        conn_id = connection['conn_id']
        # conn = session.query(Connection).filter_by(conn_id=conn_id).first()
        # if conn:
        #     session.delete(conn)
        #     session.commit()

        password: str = None
        try:
            if connection['password']:
                password = fernet.decrypt(bytes(connection['password'], 'utf-8')).decode('utf-8')
        except KeyError:
            pass
        port: int = None
        try:
            port = connection['port']
        except KeyError:
            pass
        host: str = None
        try:
            host = connection['host']
        except KeyError:
            pass
        login: str = None
        try:
            login = connection['login']
        except KeyError:
            pass
        schema: str = None
        try:
            schema = connection['schema']
        except KeyError:
            pass
        extra: str = None
        try:
            extra = connection['extra']
        except KeyError:
            pass
        uri: str = None
        try:
            uri = connection['uri']
        except KeyError:
            pass
        conn2save = Connection(
            conn_id=conn_id,
            conn_type=connection['conn_type'],
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port,
            extra=extra,
            uri=uri
        )
        session.merge(conn2save)
        session.commit()
        restored += 1
    if restored > 0:
        print("Restored Airflow [{env}] {len} connections from S3 ({uri})".format(env=env, len=restored, uri=uri))


execution_date = "2020-06-03"
_restore_variables(execution_date)
_restore_connections(execution_date)
