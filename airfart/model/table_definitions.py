import json
from abc import ABC
from typing import List


class DatabaseType(object):
    VERTICA = 'vertica'
    GLUE = 'glue'
    ATHENA = 'athena'

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
        'bigint': {DatabaseType.GLUE: 'bigint', DatabaseType.VERTICA: 'BIGINT'},
        'decimal': {DatabaseType.GLUE: 'decimal', DatabaseType.VERTICA: 'DECIMAL'},
        'double': {DatabaseType.GLUE: 'double', DatabaseType.VERTICA: 'DOUBLE PRECISION'},
        'float': {DatabaseType.GLUE: 'float', DatabaseType.VERTICA: 'FLOAT'},
        'int': {DatabaseType.GLUE: 'int', DatabaseType.VERTICA: 'INT'},
        'mediumint': {DatabaseType.GLUE: 'int', DatabaseType.VERTICA: 'INT'},
        'smallint': {DatabaseType.GLUE: 'smallint', DatabaseType.VERTICA: 'SMALLINT'},
        'tinyint': {DatabaseType.GLUE: 'tinyint', DatabaseType.VERTICA: 'TINYINT'},
        # dates
        'date': {DatabaseType.GLUE: 'date', DatabaseType.VERTICA: 'DATE'},
        'timestamp': {DatabaseType.GLUE: 'timestamp', DatabaseType.VERTICA: 'TIMESTAMP'},
        'datetime': {DatabaseType.GLUE: 'timestamp', DatabaseType.VERTICA: 'TIMESTAMP'},
        'precision_datetime': {DatabaseType.GLUE: 'precision_timestamp', DatabaseType.VERTICA: 'TIMESTAMP'},
        # characters
        'char': {DatabaseType.GLUE: 'char', DatabaseType.VERTICA: 'CHAR'},
        'varchar': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        'text': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        'tinytext': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        'longblob': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        'mediumtext': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        'longtext': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'},
        # specials
        'enum': {DatabaseType.GLUE: 'string', DatabaseType.VERTICA: 'VARCHAR'}
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

    def __eq__(self, other):
        if isinstance(other, BaseColumnDef):
            return self.name == other.name and \
                   self.ordinal_position == other.ordinal_position
        return False

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
                 data_length: str,
                 mapping: dict = None):
        super(ColumnDef, self).__init__(name, ordinal_position)
        self.data_type: str = data_type
        self.data_length: str = data_length
        self.mapping = mapping if mapping else DataTypeConverter.get_mapping(data_type)

    def __eq__(self, other):
        if isinstance(other, ColumnDef):
            return self.name == other.name and \
                   self.ordinal_position == other.ordinal_position and \
                   self.data_type == other.data_type and \
                   self.data_length == other.data_length and \
                   self.mapping == other.mapping
        return False

    @classmethod
    def from_json(cls, data: dict):
        return cls(**data)

    @classmethod
    def from_query(cls, data: dict):
        return cls.from_json(
            {
                'name': data['name'],
                'data_type': data['data_type'],
                'data_length': data['data_length'],
                'ordinal_position': data['ordinal_position']
            })


class TableDef(object):
    _parquet_package: str = 'org.apache.hadoop.hive.ql.io.parquet'

    def __init__(self,
                 name: str,
                 columns: List[ColumnDef],
                 primary_keys: List[BaseColumnDef] = None,
                 unique_keys: List[BaseColumnDef] = None):
        self.name = name
        self.columns = columns
        self.primary_keys = primary_keys
        self.unique_keys = unique_keys

    def __eq__(self, other):
        def compare(l1, l2) -> bool:
            _l1 = l1 if l1 else []
            _l2 = l2 if l2 else []
            if len(_l1) == len(_l2):
                return all(map(lambda x, y: x == y, _l1, _l2))
            return False

        if isinstance(other, TableDef):
            return self.name == other.name and \
                   compare(self.columns, other.columns) and \
                   compare(self.primary_keys, other.primary_keys) and \
                   compare(self.unique_keys, other.unique_keys)
        return False

    @classmethod
    def from_json(cls, data):
        name = data['name']
        columns = list(map(ColumnDef.from_json, data['columns']))
        primary_keys = list(map(BaseColumnDef.from_json, data['primary_keys'])) if data.get('primary_keys') else None
        unique_keys = list(map(BaseColumnDef.from_json, data['unique_keys'])) if data.get('unique_keys') else None
        return cls(name, columns, primary_keys, unique_keys)

    @classmethod
    def from_query(cls, data):
        table_name = data['TABLE_NAME']
        base_columns = sorted(json.loads(data['COLUMNS']), key=lambda x: x['ordinal_position'])
        columns = list(map(lambda c: ColumnDef.from_query(c), base_columns))
        primary_keys = list(map(lambda c: BaseColumnDef.from_query(c),
                                filter(lambda x: str(x['column_key']) == 'PRI', base_columns)))
        unique_keys = list(map(lambda c: BaseColumnDef.from_query(c),
                               filter(lambda x: str(x['column_key']) == 'UNI', base_columns)))
        return cls(table_name, columns, primary_keys, unique_keys)

    @staticmethod
    def _build_from_parameters(data, key, fn) -> list:
        return list(map(lambda x: fn(x), json.loads(data['Parameters'][key])))

    @classmethod
    def from_glue(cls, table):
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
                           'Type': x.mapping[DatabaseType.GLUE] + (
                               f'({x.data_length})' if x.mapping[DatabaseType.GLUE] != 'string' and len(
                                   x.data_length) > 0 else '')
                           }, self.columns))

        columns = TableDef._to_json(self.columns)
        primary_keys = TableDef._to_json(self.primary_keys)
        unique_keys = TableDef._to_json(self.unique_keys)
        return {
            'Name': self.name,
            'StorageDescriptor': {
                'Columns': glue_columns,
                'Location': f'{database}.{self.name}',
                'InputFormat': f'{self._parquet_package}.MapredParquetInputFormat',
                'OutputFormat': f'{self._parquet_package}.MapredParquetOutputFormat',
                'Compressed': False,
                'NumberOfBuckets': -1,
                'SerdeInfo': {
                    'SerializationLibrary': f'{self._parquet_package}.serde.ParquetHiveSerDe',
                    'Parameters': {
                        'serialization.format': '1'
                    }
                },
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
