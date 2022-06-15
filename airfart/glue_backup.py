import json
import os
from typing import List, Dict

from dotenv import load_dotenv

from airfart.operators.glue_catalog.glue_backup import GlueBackupOperator

if __name__ == '__main__':
    load_dotenv()
    bucket = os.getenv('DATA_BUCKET')
    schema: Dict[str, str] = json.loads(os.getenv('SCHEMA'))
    include_databases: List[str] = list(schema.values())
    GlueBackupOperator(bucket=bucket,
                       include_databases=include_databases).execute()
