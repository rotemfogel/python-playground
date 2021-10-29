import itertools
from typing import Set

from airfart.glue_catalog import AwsGlueCatalogHook
from airfart.s3 import AwsS3Hook


class GluePartitionCleanOperator:
    def __init__(self,
                 database_name: str,
                 table_name: str,
                 **kwargs
                 ) -> None:
        super().__init__(**kwargs)
        self.database_name = database_name
        self.table_name = table_name

    def execute(self, context=None):
        glue_hook = AwsGlueCatalogHook()
        table = glue_hook.get_table(database_name=self.database_name,
                                    table_name=self.table_name)
        partition_keys = list(map(lambda x: x['Name'], table['Table']['PartitionKeys']))
        location: str = table['Table']['StorageDescriptor']['Location']
        location_parts = location.split('/')
        bucket = location_parts[2]
        bucket_prefix = '/'.join([location_parts[3], location_parts[4]])
        partitions: Set[tuple] = glue_hook.get_partitions(database_name=self.database_name,
                                                          table_name=self.table_name,
                                                          page_size=100,
                                                          max_items=100)

        s3_hook = AwsS3Hook()
        for partition in partitions:
            # convert tuple to array
            partition_values = list(itertools.chain(partition))
            prefix = [bucket_prefix]
            for i in range(0, len(partition_values)):
                prefix.append(f'{partition_keys[i]}={partition_values[i]}')

            if not s3_hook.validate_key(bucket, '/'.join(prefix)):
                glue_hook.drop_partition(database_name=self.database_name,
                                         table_name=self.table_name,
                                         partition_values=partition_values)


if __name__ == "__main__":
    GluePartitionCleanOperator('dbl', 'page_events').execute()
