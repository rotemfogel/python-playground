import json
from abc import ABC
from contextlib import closing
from typing import List

import MySQLdb
import boto3
from airflow import AirflowException


class DatabaseType(object):
    VERTICA = 'vertica'
    GLUE = 'glue'

    _ALL_DATABASE_TYPES = set()

    @classmethod
    def is_valid(cls, db_type: str):
        return db_type.lower() in cls.all_db_types()

    @classmethod
    def all_db_types(cls):
        if not cls._ALL_DATABASE_TYPES:
            cls._ALL_DATABASE_TYPES = {
                getattr(cls, attr)
                for attr in dir(cls)
                if not attr.startswith("_") and not callable(getattr(cls, attr))
            }
        return cls._ALL_DATABASE_TYPES


class DataTypeConverter(ABC):
    _mapping = {
        # numerics
        'bigint': {DatabaseType.GLUE: 'BIGINT', DatabaseType.VERTICA: 'BIGINT'},
        'decimal': {DatabaseType.GLUE: 'DECIMAL', DatabaseType.VERTICA: 'DECIMAL'},
        'double': {DatabaseType.GLUE: 'DOUBLE', DatabaseType.VERTICA: 'DOUBLE PRECISION'},
        'float': {DatabaseType.GLUE: 'FLOAT', DatabaseType.VERTICA: 'FLOAT'},
        'int': {DatabaseType.GLUE: 'INT', DatabaseType.VERTICA: 'INT'},
        'mediumint': {DatabaseType.GLUE: 'INT', DatabaseType.VERTICA: 'INT'},
        'smallint': {DatabaseType.GLUE: 'SMALLINT', DatabaseType.VERTICA: 'SMALLINT'},
        'tinyint': {DatabaseType.GLUE: 'TINYINT', DatabaseType.VERTICA: 'TINYINT'},
        # dates
        'date': {DatabaseType.GLUE: 'DATE', DatabaseType.VERTICA: 'DATE'},
        'timestamp': {DatabaseType.GLUE: 'TIMESTAMP', DatabaseType.VERTICA: 'TIMESTAMP'},
        'datetime': {DatabaseType.GLUE: 'TIMESTAMP', DatabaseType.VERTICA: 'TIMESTAMP'},
        # characters
        'char': {DatabaseType.GLUE: 'CHAR', DatabaseType.VERTICA: 'CHAR'},
        'varchar': {DatabaseType.GLUE: 'VARCHAR', DatabaseType.VERTICA: 'VARCHAR'},
        'text': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'},
        'tinytext': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'},
        'longblob': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'},
        'mediumtext': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'},
        'longtext': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'},
        # specials
        'enum': {DatabaseType.GLUE: 'STRING', DatabaseType.VERTICA: 'VARCHAR'}
    }

    @staticmethod
    def get_mapped_column(data_type: str, target_db: DatabaseType) -> str:
        return DataTypeConverter.get_mapping(data_type)[target_db]

    @staticmethod
    def get_mapping(data_type: str) -> dict:
        return DataTypeConverter._mapping[data_type]


class BaseColumnDef(object):
    def __init__(self,
                 name: str,
                 ordinal_position: int):
        self.name: str = name
        self.ordinal_position: int = ordinal_position

    @classmethod
    def from_query(cls, data: dict):
        return cls.from_json(
            {
                'name': data['name'],
                'ordinal_position': data['ordinal_position'],
            })

    @classmethod
    def from_json(cls, data: dict):
        return cls(**data)


class ColumnDef(BaseColumnDef):
    def __init__(self,
                 name: str,
                 ordinal_position: int,
                 data_type: str,
                 mapping: dict = None):
        super(ColumnDef, self).__init__(name, ordinal_position)
        self.data_type: str = data_type
        self.mapping = mapping if mapping else DataTypeConverter.get_mapping(data_type)

    @classmethod
    def from_json(cls, data: dict):
        return cls(**data)

    @classmethod
    def from_query(cls, data: dict):
        return cls.from_json(
            {
                'name': data['name'],
                'data_type': data['data_type'],
                'ordinal_position': data['ordinal_position']
            })


class TableDef(object):
    def __init__(self,
                 name: str,
                 columns: List[ColumnDef],
                 primary_keys: List[BaseColumnDef] = None,
                 unique_keys: List[BaseColumnDef] = None):
        self.name = name
        self.columns = columns
        self.primary_keys = primary_keys
        self.unique_keys = unique_keys

    @classmethod
    def from_json(cls, data):
        name = data['name']
        columns = list(map(ColumnDef.from_json, data['columns']))
        primary_keys = list(map(BaseColumnDef.from_json, data['primary_keys'])) if data.get('primary_keys') else None
        unique_keys = list(map(BaseColumnDef.from_json, data['unique_keys'])) if data.get('unique_keys') else None
        return cls(name, columns, primary_keys, unique_keys)

    @classmethod
    def from_query(cls, data):
        table_name = data[0]
        json_columns = sorted(json.loads(data[1]), key=lambda x: x['ordinal_position'])
        columns = list(map(lambda c: ColumnDef.from_query(c), json_columns))
        primary_keys = list(map(lambda c: BaseColumnDef.from_query(c),
                                filter(lambda x: str(x['column_key']) == 'PRI', json_columns)))
        unique_keys = list(map(lambda c: BaseColumnDef.from_query(c),
                               filter(lambda x: str(x['column_key']) == 'UNI', json_columns)))
        return cls(table_name, columns, primary_keys, unique_keys)

    @staticmethod
    def _build_from_parameters(data, key, fn) -> list:
        return list(map(lambda x: fn(x), json.loads(data['Parameters'][key])))

    @classmethod
    def from_glue(cls, data):
        table = data['Table']
        name = table['Name']
        columns = TableDef._build_from_parameters(table, 'columns', ColumnDef.from_json)
        primary_keys = TableDef._build_from_parameters(table, 'primary_keys', BaseColumnDef.from_json)
        unique_keys = TableDef._build_from_parameters(table, 'unique_keys', BaseColumnDef.from_json)
        return cls(name, columns, primary_keys, unique_keys)

    @staticmethod
    def _to_json(data: list) -> str:
        return json.dumps(data, default=lambda o: o.__dict__) if data else '[]'

    def to_glue(self, database: str) -> dict:
        glue_columns = list(
            map(lambda x: {'Name': x.name,
                           'Type': x.mapping[DatabaseType.GLUE]
                           }, self.columns))

        columns = TableDef._to_json(self.columns)
        primary_keys = TableDef._to_json(self.primary_keys)
        unique_keys = TableDef._to_json(self.unique_keys)
        return {
            'Name': self.name,
            'StorageDescriptor': {
                'Columns': glue_columns,
                'Location': f'{database}.{self.name}',
                'Compressed': False,
                'NumberOfBuckets': -1,
                'SerdeInfo': {'Parameters': {}},
                'BucketColumns': [],
                'SortColumns': [],
                'StoredAsSubDirectories': False,
            },
            'PartitionKeys': [],
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'mysql',
                'columns': columns,
                'primary_keys': primary_keys,
                'unique_keys': unique_keys,
            },
        }


def _write(records: list, file: str, records_transform_fn=None) -> None:
    with open(file, 'wb') as f:
        for record in records:
            if not record:
                continue
            record_to_write = records_transform_fn(record) if records_transform_fn else record
            f.write((json.dumps(record_to_write, default=lambda o: o.__dict__) + '\n').encode('utf8'))


def _read(file, records_transform_fn=None) -> list:
    with open(file, 'r') as f:
        file_lines = []
        for line in f.readline():
            file_lines.append(records_transform_fn(line) if records_transform_fn else line)
        return file_lines


def _get_conn():
    from dotenv import load_dotenv
    load_dotenv()
    import os
    conn_config = {
        'host': os.getenv('PRODUCTION_HOST'),
        'user': os.getenv('PRODUCTION_USER'),
        'password': os.getenv('PRODUCTION_PASSWORD'),
        'database': os.getenv('PRODUCTION_DB'),
        'port': int(os.getenv('PRODUCTION_PORT'))
    }
    return MySQLdb.connect(**conn_config)


def _query(sql: str,
           set_session_variable=None) -> list:
    # set session variables before executing the query
    # Sending query to mysql database and fetching results
    if set_session_variable is None:
        set_session_variable = {}
    with closing(_get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            if set_session_variable:
                for k, v in set_session_variable.items():
                    set_session = f'SET SESSION {k}={v}'
                    cur.execute(set_session)
            cur.execute(sql)
            return cur.fetchall()


_hook = boto3.session.Session(region_name='us-west-2').client('glue')


def _get_table(db: str, table: str) -> TableDef:
    table = _hook.get_table(DatabaseName=db, Name=table)
    return TableDef.from_glue(table)


def _drop_table(db: str, table: str):
    # drop table if exists
    try:
        _hook.delete_table(DatabaseName=db,
                           Name=table)
    except Exception as err:
        if 'EntityNotFoundException' in str(err):
            pass
        else:
            print("delete_table found an error : |" + str(err))
            raise AirflowException("|\n\nManual Exception, with delete_table. the Exception is: " + str(err))


def _create_table(db: str, table_def: TableDef):
    table_input = table_def.to_glue(db)
    try:
        _hook.create_table(DatabaseName=db,
                           TableInput=table_input)
    except Exception as err:
        if 'AlreadyExistsException' in str(err):
            pass
        else:
            print("create_table found an error : " + str(err))
            raise AirflowException("|\n\nManual Exception, with delete_table. the Exception is: " + str(err))


production_db = "seekingalpha_production"


def _create_glue_table(s: str) -> None:
    table_def = TableDef.from_json(json.loads(s))
    _drop_table(production_db, table_def.name)
    _create_table(production_db, table_def)
    table = _get_table(production_db, table_def.name)
    print(json.dumps(table, default=lambda o: o.__dict__, indent=2))


_session_variables = {'group_concat_max_len': 1000000}
if __name__ == '__main__':
    with open('mysql_schema_dump.sql') as m:
        query = m.read()
    raw_records = _query(query, _session_variables)
    _write(raw_records, 'records.json')
    _write(raw_records, 'tables.json', TableDef.from_query)
    with open('tables.json', 'r') as r:
        lines = r.read().split('\n')
        for the_line in lines:
            _create_glue_table(the_line)
