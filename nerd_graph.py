import json
import os

import dotenv
import requests

dotenv.load_dotenv()


def nerdgraph_nrql(key, account, query):
    # GraphQL query to NerdGraph
    query = """
  { 
    actor { account(id: %s) 
      { nrql
      (query: "%s") 
      { results } } } 
  }""" % (
        account,
        query,
    )

    # NerdGraph endpoint
    endpoint = "https://api.newrelic.com/graphql"
    headers = {"API-Key": f"{key}"}
    response = requests.post(endpoint, headers=headers, json={"query": query})

    if response.status_code == 200:
        # convert a JSON into an equivalent python dictionary
        dict_response = json.loads(response.content)
        print(dict_response["data"]["actor"]["account"]["nrql"])

        # optional - serialize object as a JSON formatted stream
        # json_response = json.dumps(response.json(), indent=2)
        # print(json_response)

    else:
        # raise an exepction with a HTTP response code, if there is an error
        raise Exception(f"Nerdgraph query failed with a {response.status_code}.")


if __name__ == "__main__":
    key = os.getenv("NEWRELIC_LICENSE")
    account = os.getenv("NEWRELIC_ACCOUNT")
    query = os.getenv("NEWRELIC_QUERY")
    nerdgraph_nrql(key, account, query)
