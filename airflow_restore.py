import json

from airflow import settings
from airflow.models import Variable, Connection
from airflow.models.crypto import get_fernet
from smart_open import open

env = Variable.get('env')
data_bucket = Variable.get('data_bucket')
fernet = get_fernet()
session = settings.Session()


def _read_from_s3(uri):
    records = []
    with open(uri=uri, mode='rb') as s3_file:
        for line in s3_file:
            records.append()
    return records


def _restore_variables(execution_date):
    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='variables', )

    raw_variables = _read_from_s3(uri)
    variables = json.loads(''.join(raw_variables))
    for k, v in variables.items():
        Variable.set(k, v)

    print("Restored Airflow [{env}] {len} variables to S3 ({uri})".format(env=env, len=len(variables), uri=uri))


def _restore_connections(execution_date):
    uri = 's3://{bucket}/airflow-backup/{env}/date_={date}/{what}.json.gz'.format(bucket=data_bucket,
                                                                                  env=env,
                                                                                  date=execution_date,
                                                                                  what='connections', )
    raw_connections = _read_from_s3(uri)
    connections = json.loads(''.join(raw_connections))

    for connection in connections:
        conn_id = conn_id = connection['conn_id']
        conn_exists = session.query(Connection).filter_by(conn_id=conn_id).first()
        if not conn_exists:
            conn2save = Connection(
                conn_id=conn_id,
                extra=connection['extra'],
                is_extra_encrypted=connection['is_extra_encrypted'],
                conn_type=connection['conn_type'],
                login=connection['login'],
                password=connection['password'],
                schema=connection['schema'],
                port=connection['port'],
            )
            session.save(conn2save)

    print("Backed up Airflow [{env}] connections to S3 ({uri})".format(env=env, uri=uri))


execution_date = "2020-06-01"
_restore_variables(execution_date)
_restore_connections(execution_date)
