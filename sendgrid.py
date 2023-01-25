import json
import os
from urllib import parse

import requests
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
from pendulum import DateTime
from smart_open import open

load_dotenv()


class SendGridOperator:
    _endpoint = os.getenv("SENDGRID_ENDPOINT")

    def __init__(self, bucket: str, schema: str, table: str, execution_date: DateTime):
        self._schema = schema
        self._table = table
        self._bucket = bucket
        self._secret = os.getenv("SENDGRID_SECRET")
        self._execution_date = execution_date

    def execute(self):
        # get the 5 minute interval
        start_time = self._execution_date.format("%Y-%m-%dT%H:%M:%SZ")
        end_time = (
            self._execution_date.add(minutes=4)
            .add(seconds=59)
            .format("%Y-%m-%dT%H:%M:%SZ")
        )
        params = {
            "query": 'last_event_time BETWEEN TIMESTAMP "{}" AND TIMESTAMP "{}"'.format(
                start_time, end_time
            )
        }
        query_params = "&".join(
            list(
                map(
                    lambda kv: ("{}={}".format(kv[0], parse.quote(kv[1]))),
                    params.items(),
                )
            )
        )
        headers = {"Authorization": "Bearer {}".format(self._secret)}
        print("Sending GET payload {} to endpoint {}".format(params, self._endpoint))
        endpoint = "&".join([self._endpoint, query_params])
        file_name = str(start_time).replace(":", "-")
        retries = 3
        ix = 0
        while ix < retries:
            try:
                response = requests.request("GET", endpoint, data={}, headers=headers)
                if response.status_code == 200:
                    json_response = response.json()
                    if json_response:
                        date = self._execution_date.format("%Y-%m-%d")
                        uri = "s3://{bucket}/{schema}/{table}/date_={date}/{file_name}.json.gz".format(
                            bucket=self._bucket,
                            schema=self._schema,
                            table=self._table,
                            date=date,
                            file_name=file_name,
                        )
                        messages = json_response["messages"]
                        if messages:
                            with open(uri=uri, mode="wb") as s3_file:
                                print("About to write response to {}".format(uri))
                                for message in messages:
                                    if message:
                                        s3_file.write(
                                            (json.dumps(message) + "\n").encode()
                                        )
                            print("Uploaded {} to S3".format(self._table))
                        else:
                            print(
                                "Received empty response for payload {}".format(params)
                            )
                        ix = 3  # end loop
                else:
                    ix += 1
            except json.decoder.JSONDecodeError as ex:
                print(format(ex))
                ix += 1
        if ix == 2:  # reached the max retries
            raise AirflowException(
                "an error occurred sending GET payload {} to {}:\n{}".format(
                    self._endpoint, params, response.json()
                )
            )


if __name__ == "__main__":
    ed = DateTime(2021, 2, 22, 20, 40, 0)
    sendgrid = SendGridOperator("bucket", "schema", "table", ed)
    sendgrid.execute()
