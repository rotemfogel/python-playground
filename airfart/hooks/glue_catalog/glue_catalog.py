import inspect
from typing import List, Optional

import boto3
from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.model.table_definitions import TableDef


class GlueCatalogHook(LoggingMixin):
    def __init__(self, region: str = "us-west-2"):
        self.region = region
        session = boto3.session.Session(region_name=self.region)
        self._client = session.client("glue")

    def __chunk(self, what: List[object]) -> List[List[object]]:
        # create chunks of 100 from a list
        return [
            what[i * self._chunk_size : (i + 1) * self._chunk_size]
            for i in range((len(what) + self._chunk_size - 1) // self._chunk_size)
        ]

    def _paginate(
        self, operation: str, page_key: str, value_key: Optional[str] = None, **kwargs
    ) -> list:
        paginator = self.get_conn().get_paginator(operation)
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

    def get_conn(self):
        return self._client

    def get_databases(self) -> list:
        return self._paginate("get_databases", "DatabaseList", "Name")

    def get_database(self, db: str) -> Optional[dict]:
        try:
            args = {"Name": db}
            return self.get_conn().get_database(**args)
        except Exception as e:
            if "EntityNotFoundException" in str(e):
                return None
            raise e

    def get_tables(self, db: str) -> Optional[list]:
        try:
            args = {"DatabaseName": db}
            return self._paginate("get_tables", "TableList", "Name", **args)
        except Exception as e:
            if "EntityNotFoundException" in str(e):
                return None
            raise e

    def get_table(self, db: str, table: str) -> dict:
        """
        Get the information of the table

        :param db: Name of hive database (schema) @table belongs to
        :param table: Name of hive table
        :rtype: dict
        """
        result = self.get_conn().get_table(DatabaseName=db, Name=table)
        return result["Table"]

    def get_table_versions(self, db: str, table: str) -> Optional[list]:
        try:
            args = {"DatabaseName": db, "TableName": table}
            return self._paginate(
                "get_table_versions", "TableVersions", "VersionId", **args
            )
        except Exception as e:
            if "EntityNotFoundException" in str(e):
                return None
            raise e

    def delete_tables(self, db: str, tables_to_remove: List[str]) -> None:
        # create chunks of 100 tables to drop
        table_chunks = self.__chunk(tables_to_remove)
        for chunk in table_chunks:
            self.get_conn().batch_delete_table(
                DatabaseName=db, TablesToDelete=list(map(lambda x: str(x), chunk))
            )
        self.log.info(f"dropped {len(tables_to_remove)} tables from database {db}")

    def delete_table_versions(self, db: str, table: str) -> None:
        # convert results to int and sort
        versions = sorted(
            list(map(lambda x: int(x), self.get_table_versions(db, table)))
        )
        if len(versions) > self._chunk_size:
            versions_to_drop = versions[: -self._chunk_size]
            # create chunks of 100 versionIds to drop
            version_chunks = self.__chunk(versions_to_drop)
            for chunk in version_chunks:
                self.get_conn().batch_delete_table_version(
                    DatabaseName=db,
                    TableName=table,
                    VersionIds=list(map(lambda x: str(x), chunk)),
                )
            self.log.info(
                f"dropped {len(versions_to_drop)} versions out of {len(versions)} from table {db}.{table}"
            )

    def delete_all_table_versions(self, db: str) -> None:
        # check if database exists first
        if self.get_database(db):
            # filter table starting with `tmp_` -> temporary process_engine tables
            tables = list(
                filter(lambda x: not str(x).startswith("tmp_"), self.get_tables(db))
            )
            for table in tables:
                self.delete_table_versions(db, table)

    def get_table_input(self, database: str, table: str) -> dict:
        # Meaning, requesting to generate a request object to update a Table on AWS Data Catalog
        table_result = self.get_table(database, table)
        table_name = table_result["Name"]
        table_input = dict(
            Name=table_name, StorageDescriptor=table_result["StorageDescriptor"]
        )
        # Required table input parameters
        table_input["StorageDescriptor"]["SerdeInfo"]["Name"] = table_result.get(
            "SerdeInfo", {}
        ).get("Name", table_name)
        table_input["TableType"] = table_result.get("TableType", "EXTERNAL_TABLE")
        table_input["Parameters"] = table_result.get(
            "Parameters", {"EXTERNAL": "TRUE", "has_encrypted_data": "false"}
        )
        # if partition keys exist, add to `table_input`
        partition_keys = "PartitionKeys"
        table_input[partition_keys] = table_result.get(partition_keys, [])
        return table_input

    def glue_add_columns(self, database, table, columns):
        table_schema = self.get_table_input(database, table)
        table_schema["StorageDescriptor"]["Columns"].extend(columns)
        self.log.info(
            "\n\ntable_schema['StorageDescriptor']['Columns']:\n"
            + str(table_schema["StorageDescriptor"]["Columns"])
        )
        try:
            self.get_conn().update_table(DatabaseName=database, TableInput=table_schema)
            self.log.info(
                "Columns: {cols} added to the table: {db}.{tbl}".format(
                    db=database, tbl=table, cols=columns
                )
            )
        except Exception as err:
            raise Exception(
                "Manually Exception, unable to add column to table: {db}.{table} the Exception is: {er}".format(
                    db=database, table=table, er=str(err)
                )
            )

    def get_glue_list_columns(self, database, table) -> list:
        glue_schema = self.get_table_input(database, table)
        list_glue_cols_raw = glue_schema.get("StorageDescriptor").get("Columns")
        curr_list = []
        for num, col in enumerate(list_glue_cols_raw):
            curr_list.append(
                {
                    **{"row_num": num},
                    **dict((k.strip(), v.strip()) for k, v in col.items()),
                }
            )
        return curr_list

    def create_table(self, database_name, table_input):
        result = self.get_conn().create_table(
            DatabaseName=database_name, TableInput=table_input
        )
        return result

    def update_table(self, database_name, table_input):
        result = self.get_conn().update_table(
            DatabaseName=database_name, TableInput=table_input
        )
        return result

    def get_table_def(self, database_name: str, table_name: str) -> Optional[TableDef]:
        try:
            table_def = self.get_table(
                database_name=database_name, table_name=table_name
            )
            return TableDef.from_glue(table_def)
        except Exception as err:
            if "EntityNotFoundException" in str(err):
                return None
            else:
                self.raise_error(inspect.currentframe().f_code.co_name, err)

    def create_table_def(self, database_name, table_def: TableDef):
        table_input = table_def.to_glue(database_name)
        try:
            self.create_table(database_name=database_name, table_input=table_input)
            self.log.info(f"created table {database_name}.{table_def.name}")
        except Exception as err:
            if "AlreadyExistsException" in str(err):
                pass
            else:
                self.raise_error(inspect.currentframe().f_code.co_name, err)

    def update_table_def(self, database_name, table_def: TableDef):
        table_input = table_def.to_glue(database_name)
        try:
            self.update_table(database_name=database_name, table_input=table_input)
            self.log.info(f"updated table {database_name}.{table_def.name}")
        except Exception as err:
            self.raise_error(inspect.currentframe().f_code.co_name, err)

    def raise_error(self, calling_method: str, err: Exception):
        self.log.error(f"{calling_method} found an error : " + str(err), exc_info=True)
        raise Exception(
            f"|\n\nManual Exception, with {calling_method}. the Exception is: {str(err)}"
        )
