import argparse
import json
import os
import time
from typing import Optional

import boto3
from dotenv import load_dotenv


class AthenaQueryExecutor:
    def __init__(self, sql: Optional[str] = None, query: Optional[str] = None) -> None:
        super().__init__()
        self.sql = sql
        self.query = query
        self.__client = None

    def __get_client(self):
        if not self.__client:
            self.__client = boto3.Session().client("athena")
        return self.__client

    def __query(self, query: str, conf: dict):
        client = self.__get_client()
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": conf["database"]},
            ResultConfiguration={
                "OutputLocation": "s3://" + conf["bucket"] + "/" + conf["path"]
            },
        )
        execution_id = response["QueryExecutionId"]
        state = "RUNNING"

        while state in ["RUNNING", "QUEUED"]:
            response = client.get_query_execution(QueryExecutionId=execution_id)
            if (
                "QueryExecution" in response
                and "Status" in response["QueryExecution"]
                and "State" in response["QueryExecution"]["Status"]
            ):
                status = response["QueryExecution"]["Status"]
                state = status["State"]
            if state == "FAILED":
                reason = status["StateChangeReason"]
                if "AlreadyExistsException" in reason:
                    break
                raise Exception(f"query [{query}] failed: {reason}")
            elif state == "SUCCEEDED":
                break
            print(state)
            time.sleep(1)

    def execute(self):
        load_dotenv()
        params = json.loads(os.getenv("ATHENA_PARAMS"))

        if self.query:
            queries = [self.query]
        else:  # self.sql
            with open(self.sql, "r") as f:
                queries = f.read().split("\n")

        for query in queries:
            if query:
                print(query)
                self.__query(query, params)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Athena query executor")
    parser.add_argument("--sql", help="sql file to process")
    parser.add_argument("--query", help="query to process")
    args = parser.parse_args()
    assert args.sql or args.query, "must provide either [sql] or [query] parameter"
    assert not (
        args.sql and args.query
    ), "cannot provide both [sql] or [query] parameter"
    if args.sql:
        assert args.sql[-4:] == ".sql", "file must end with .sql"
    qe = AthenaQueryExecutor(sql=args.sql, query=args.query)
    qe.execute()
