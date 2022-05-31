import json
import os

from airflow import AirflowException
from dotenv import load_dotenv

from airfart.everflow.everflow_operator import EverFlowOperator

if __name__ == '__main__':
    load_dotenv()
    try:
        records_str = os.getenv('everflow_records')
        records = json.loads(records_str)
        everflow = EverFlowOperator(records=records)
        everflow.execute()
    except AirflowException:
        pass

    try:
        records_str = os.getenv('everflow_historical_records')
        records = json.loads(records_str)
        everflow = EverFlowOperator(records=records, report_click=False)
        everflow.execute()
    except AirflowException:
        pass
