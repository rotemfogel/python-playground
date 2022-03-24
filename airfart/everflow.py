import json
import os

from dotenv import load_dotenv

from airfart.everflow.everflow_operator import EverFlowOperator

if __name__ == '__main__':
    load_dotenv()
    records_str = os.getenv('everflow_records')
    records = json.loads(records_str)
    everflow = EverFlowOperator(records=records_str)
    everflow.execute()
    everflow = EverFlowOperator(records=records)
    everflow.execute()
