import inspect
import json
import time
from typing import Optional

import boto3
import smart_open
from airflow import AirflowException

from mysql_schema_dump import TableDef


def raise_error(calling_method: str, err: Exception):
    print(f'{calling_method} found an error : ' + str(err))
    raise AirflowException(f'|\n\nManual Exception, with {calling_method}. the Exception is: {str(err)}')


class S3ToGlueOperator:
    default_target_db = 'seekingalpha_production'

    def __init__(self,
                 file: str,
                 bucket: str,
                 key: str,
                 post_key_path=None,
                 database: str = default_target_db):
        self.file = file
        self.bucket = bucket
        self.key = key
        # strip leading trailing backslashes
        self.post_key_path = post_key_path.strip('/') if post_key_path else post_key_path
        self.database = database
        self._hook = boto3.Session(profile_name='default', region_name='us-west-2').client('glue')

    def execute(self):
        # Asserting whether daily partitions are required or a rewritable file
        if self.post_key_path:
            uri = 's3://{bucket}/{key}/{post_key_path}/{file}.json.gz'.format(bucket=self.bucket,
                                                                              key=self.key,
                                                                              post_key_path=self.post_key_path,
                                                                              file=self.file)
        else:
            uri = 's3://{bucket}/{key}/{file}.json.gz'.format(bucket=self.bucket,
                                                              key=self.key,
                                                              file=self.file)
        print(f'about to restore file {uri}')
        changed_tables = []
        tables = []
        with smart_open.open(uri, 'r') as s3_file:
            for line in s3_file:
                tables.append(TableDef.from_json(json.loads(line)))

        while len(tables) > 0:
            # get current table implementation and compare
            table_def = tables.pop()
            retries = 5
            retry = 0
            while retry < retries:
                try:
                    curr_table = self._get_table(table_def.name)
                    if curr_table:
                        if table_def == curr_table:
                            print(f'Skipping table {table_def.name} - no change')
                        else:
                            self._update_table(table_def)
                            changed_tables.append(table_def.name)
                    else:
                        self._create_table(table_def)
                    retry = retries
                except Exception as err:
                    if 'ThrottlingException' in str(err):
                        retry += 1
                        if retry <= retries:
                            time.sleep(retry ^ 2)
                        else:
                            raise err
                    else:
                        raise err

        return changed_tables

    def _get_table(self, table_name: str) -> Optional[TableDef]:
        try:
            table_def = self._hook.get_table(DatabaseName=self.database,
                                             Name=table_name)
            return TableDef.from_glue(table_def)
        except Exception as err:
            if 'EntityNotFoundException' in str(err):
                return None
            else:
                raise_error(inspect.currentframe().f_code.co_name, err)

    def _create_table(self, table_def: TableDef):
        table_input = table_def.to_glue(self.database)
        try:
            self._hook.create_table(DatabaseName=self.database,
                                    TableInput=table_input)
            print(f'created table {self.database}.{table_def.name}')
        except Exception as err:
            if 'AlreadyExistsException' in str(err):
                pass
            else:
                raise_error(inspect.currentframe().f_code.co_name, err)

    def _update_table(self, table_def: TableDef):
        table_input = table_def.to_glue(self.database)
        try:
            self._hook.update_table(DatabaseName=self.database,
                                    TableInput=table_input)
            print(f'updated table {self.database}.{table_def.name}')
        except Exception as err:
            raise_error(inspect.currentframe().f_code.co_name, err)


if __name__ == "__main__":
    S3ToGlueOperator(file='mariadb_schema_dump',
                     bucket='seekingalpha-data',
                     key='dba',
                     post_key_path='mariadb_schema_dump/date_=2021-03-08/hour=05',
                     database='seekingalpha_production').execute()
