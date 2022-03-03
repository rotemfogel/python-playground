import datetime
import os

from dotenv import load_dotenv

from airfart.dbapi.operators.mysql_to_s3 import MySQLToS3Operator
from airfart.model.table_definitions import TableDef

if __name__ == '__main__':
    load_dotenv()
    dag_id = 'mariadb_schema_dump'
    with open(f'{dag_id}.sql', 'r') as f:
        sql = ''.join(f.readlines())
    now = datetime.datetime.now()
    operator = MySQLToS3Operator(task_id=dag_id,
                                 sql=sql,
                                 # always write from production
                                 bucket=os.getenv('DATA_BUCKET'),
                                 # always write to production
                                 database='dba_rotem',
                                 file_name=dag_id,
                                 # always query production
                                 db_conn_id='xxx',
                                 post_db_path='/'.join(
                                     [dag_id, f'date_={now.strftime("%Y-%m-%d")}/hour={now.strftime("%H")}']),
                                 records_transform_fn=TableDef.from_query,
                                 session_variables={'group_concat_max_len': 1000000})
    operator.execute(context=None)
