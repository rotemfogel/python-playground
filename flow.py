import json

from airflow import AirflowException
from airflow import settings
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable, Connection

projects = Variable.get("delighted", deserialize_json=True)
endpoint = "/v1/survey_responses.json"
session = settings.Session()

for project in projects:
    for table, secret in project.items():
        task_id = "delighted_{}".format(table)
        delighted_conn = Connection(
            conn_id=task_id,
            conn_type="http",
            host="https://api.delighted.com",
            login=secret,
        )
        session_exists = session.query(Connection).filter_by(conn_id=task_id).first()
        if not session_exists:
            session.add(delighted_conn)
            session.commit()
        hook = HttpHook(method="GET", http_conn_id=delighted_conn.conn_id)
        page_num = 1  # initialize
        payload = {"page": page_num, "per_page": 100}
        records = []
        while payload["page"] < 2:
            print(
                "{}: Sending payload: {} to endpoint {}".format(
                    task_id, payload, endpoint
                )
            )
            response = hook.run(endpoint=endpoint, data=payload)
            if (response.status_code == 200) | (response.status_code == 404):
                try:
                    elements = response.json()
                    if not elements:
                        break
                    for element in elements:
                        if not element:
                            continue
                        # flatten person_properties value if exists
                        if element["person_properties"]:
                            element.update(element["person_properties"])
                            del element["person_properties"]
                        # fix problematic fields
                        record = {}
                        for k, v in element.items():
                            record.update({k.replace(" ", "_").lower(): v})
                        print(json.dumps(record))
                        print(json.dumps(element))
                        records.append(record)
                    payload["page"] += 1  # increment last id
                except AttributeError:
                    break
            else:
                error_msg = (
                    "Got response status code {} for payload: {} to endpoint {}".format(
                        response.status_code, payload["page"], endpoint
                    )
                )
                print(error_msg)
                raise AirflowException(error_msg)
