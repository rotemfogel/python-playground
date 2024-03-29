import os

import vertica_python
from dotenv import load_dotenv


def _calculate_correct_license(results: list, ex_tab_license: int):
    parts = results[-1][-1].split("\n")
    overall_usage = int(float(parts[0].split(" ")[3].replace("TB", "")) * 1024)
    actual_license = overall_usage - ex_tab_license
    total_license = int(float(parts[1].split(" ")[-1].replace("TB", "")) * 1024)
    return int((actual_license / total_license) * 100)


if __name__ == "__main__":
    load_dotenv()

    conn_info = {
        "host": os.getenv("VERTICA_HOST"),
        "port": 5433,
        "user": os.getenv("VERTICA_USER"),
        "password": os.getenv("VERTICA_PASSWORD"),
        "database": os.getenv("VERTICA_DATABASE"),
        # autogenerated session label by default,
        "session_label": "license check",
        # default throw error on invalid UTF-8 results
        "unicode_error": "strict",
        # SSL is disabled by default
        "ssl": False,
        # autocommit is off by default
        "autocommit": False,
        # using server-side prepared statements is disabled by default
        "use_prepared_statements": False,
        # connection timeout is not enabled by default
        # 5 seconds timeout for a socket operation (Establishing a TCP connection or read/write operation)
        "connection_timeout": 15,
    }

    # using with for auto connection closing after usage
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.export_data(os.getenv("VERTICA_EXTERNAL_TABLE_QUERY"))
        ex_table_license = int(cur.fetchone()[0])
        cur.export_data(os.getenv("VERTICA_COMPLIANCE_QUERY"))
        compliance = cur.fetchall()

    print(_calculate_correct_license(compliance, ex_table_license))
