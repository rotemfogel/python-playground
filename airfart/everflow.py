import os

import pendulum
import vertica_python
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
from pendulum import Date

from airfart.operators.everflow.everflow_operator import EverFlowOperator

if __name__ == "__main__":
    load_dotenv()

    start: Date = pendulum.Date(2022, 6, 3)
    until: Date = pendulum.Date.today().add(days=1)

    diff = start.diff(until).in_days()
    dates = [start.add(days=i) for i in range(0, diff)]

    conn_info = {
        "host": os.getenv("VERTICA_HOST"),
        "port": 5433,
        "user": os.getenv("VERTICA_USER"),
        "password": os.getenv("VERTICA_PASSWORD"),
        "database": os.getenv("VERTICA_DATABASE"),
        "session_label": "everflow",
        "unicode_error": "strict",
        "ssl": False,
        "autocommit": True,
        "connection_timeout": 5,
    }
    sql = os.getenv("EVERFLOW_SQL")

    connection = vertica_python.connect(**conn_info)
    try:
        for dt in dates:
            print(f"executing everflow API for {dt}")
            cur = None
            try:
                # raw_records = os.getenv('everflow_records')
                # records = json.loads(raw_records)
                cur = connection.cursor()
                cur.execute(f"{sql} AND date_ = '{dt.format('YYYY-MM-DD')}'::date")
                records = cur.fetchall()
                everflow = EverFlowOperator(records=records)
                everflow.execute()
            except AirflowException:
                pass
            finally:
                cur.close()

            try:
                # raw_records = os.getenv('everflow_historical_records')
                # records = json.loads(raw_records)
                cur = connection.cursor()
                cur.execute(
                    f"{sql} AND date_ = '{dt.subtract(days=17).format('YYYY-MM-DD')}'::date"
                )
                records = cur.fetchall()
                everflow = EverFlowOperator(records=records, report_click=False)
                everflow.execute()
            except AirflowException:
                pass
            finally:
                cur.close()
    finally:
        connection.close()
