import json

import pandas as pd
import psycopg2
import smart_open


class PostgresToS3Operator:
    JSON: str = 'json'
    PARQUET: str = 'parquet'
    CSV: str = 'csv'
    _allowed_formats = [JSON, PARQUET, CSV]

    def __init__(
            self,
            sql: str,
            bucket: str,
            database: str,
            file_name: str,
            output_format: str = JSON,
            include_csv_headers: bool = False,
            post_db_path: str = None):
        self.sql = sql
        self.bucket = bucket
        self.database = database
        self.file_name = file_name
        self.output_format = output_format
        assert self.output_format in self._allowed_formats, f'output_format should be either {self.JSON}, {self.PARQUET} or {self.CSV}! '
        self.post_db_path = post_db_path
        self.include_csv_headers = include_csv_headers

    def execute(self):
        # Generate write destination
        path_components = ['s3:/', self.bucket, self.database, self.post_db_path, self.file_name]
        path_components = [p_c for p_c in path_components if p_c]  # removing None segments
        if self.output_format == self.JSON:
            suffix = '.json.gz'
        elif self.output_format == self.CSV:
            suffix = '.csv'
        else:
            suffix = '.parquet'
        address = '/'.join(path_components) + suffix
        print(f"\nGenerated destination: {address}\n")

        # Query logging
        print('\nExecuting the following query: %s\n', self.sql)

        # Establishing connection and fetching query results
        conn_args = dict(
            host='localhost',
            user='airflow',
            password='airflow',
            dbname='airflow',
            port=5432)

        conn = psycopg2.connect(**conn_args)

        df = pd.read_sql(self.sql, conn)

        if self.output_format == self.PARQUET:
            df.to_parquet(address, engine='pyarrow', allow_truncated_timestamps=True)
        else:
            if self.output_format == self.JSON:
                columns = df.select_dtypes(include=['datetime64']).columns
                for column in columns:
                    df[column] = df[column].astype(str)
                values = list(map(lambda x: json.dumps(x), df.to_dict(orient="records")))
            else:  # self.CSV
                values = df.to_csv(index=False, header=self.include_csv_headers).split("\n")

            # Copy to S3
            print(f'about to write to file {address}')
            with smart_open.smart_open(address, 'wb') as s3_file:
                for record in values:
                    if not record:
                        continue
                    s3_file.write(f'{record}\n'.encode('utf8'))

        print('All done')


if __name__ == '__main__':
    for output_format in [PostgresToS3Operator.CSV, PostgresToS3Operator.JSON]:
        PostgresToS3Operator(sql='select * from catalog_updates',
                             bucket='seekingalpha-data-dev',
                             database='test_dbr',
                             file_name='catalog_updates',
                             post_db_path='catalog_updates',
                             include_csv_headers=True,
                             output_format=output_format).execute()
