from typing import List

import pendulum
import smart_open
from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.hooks.glue_catalog.glue_catalog import GlueCatalogHook
from airfart.model.table_definitions import TableDef


class GlueBackupOperator(LoggingMixin):
    template_fields = ('bucket', 'exclude_databases', 'exclude_tables')

    ui_color = '#b025ae'

    def __init__(self,
                 *,
                 bucket: str,
                 include_databases: List[str] = None,
                 exclude_tables: List[str] = None,
                 region: str = 'us-west-2',
                 **kwargs):
        super(GlueBackupOperator, self).__init__(**kwargs)
        self.bucket = bucket
        self.exclude_tables = exclude_tables if exclude_tables else []
        self.include_databases = include_databases if include_databases else []
        self.region = region

    def get_hook(self):
        return GlueCatalogHook(region=self.region)

    def execute(self):
        date = pendulum.Date.today().format('YYYY-MM-DD')
        hook = self.get_hook()
        databases = self.include_databases if self.include_databases else hook.get_databases()
        for db in databases:
            uri = f's3://{self.bucket}/glue-catalog-backup/date_={date}/db={db}/{date}_{db}.json.gz'
            with smart_open.smart_open(uri, 'wb') as s3_file:
                tables: List[str] = hook.get_tables(db)
                if tables:
                    backed_up: int = 0
                    for table in tables:
                        # skip tables
                        if table not in self.exclude_tables and (not table.startswith('tmp_')):
                            self.log.debug(f'backing up table {db}.{table}')
                            glue_table = hook.get_table(db, table)
                            table_for_backup = TableDef.from_glue(glue_table)
                            s3_file.write((TableDef.to_json(table_for_backup) + '\n').encode('utf8'))
                            backed_up += 1
                        else:
                            self.log.warning(f'skipping table {db}.{table}')
                    self.log.info(f'exported {backed_up} tables from database [{db}]')
        self.log.info('export done')
