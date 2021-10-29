from typing import Optional, Set

from airfart.base_aws_hook import BaseAwsHook


class AwsGlueCatalogHook(BaseAwsHook):

    def __init__(self, ) -> None:
        super().__init__('glue')

    def get_partitions(
            self,
            database_name: str,
            table_name: str,
            expression: str = '',
            page_size: Optional[int] = None,
            max_items: Optional[int] = None,
    ) -> Set[tuple]:
        """
        Retrieves the partition values for a table.

        :param database_name: The name of the catalog database where the partitions reside.
        :type database_name: str
        :param table_name: The name of the partitions' table.
        :type table_name: str
        :param expression: An expression filtering the partitions to be returned.
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
        :type expression: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: set of partition values where each value is a tuple since
            a partition may be composed of multiple columns. For example:
            ``{('2018-01-01','1'), ('2018-01-01','2')}``
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('get_partitions')
        response = paginator.paginate(
            DatabaseName=database_name, TableName=table_name, Expression=expression, PaginationConfig=config
        )

        partitions = set()
        for page in response:
            for partition in page['Partitions']:
                partitions.add(tuple(partition['Values']))

        return partitions

    def get_table(self, database_name: str, table_name: str):
        return self.get_conn().get_table(DatabaseName=database_name, Name=table_name)

    def drop_partition(self, database_name, table_name, partition_values):
        self.get_conn().delete_partition(DatabaseName=database_name,
                                         TableName=table_name,
                                         PartitionValues=partition_values)
