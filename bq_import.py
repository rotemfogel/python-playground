import argparse

from bigquery.bigquery_client_executor import BigQueryClientExecutor

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="infer_ab_cart_model",
        description="Infer Abandoned Cart Recommendations",
        epilog="Thank you for inferring carts",
    )
    parser.add_argument("-p", "--project")
    parser.add_argument("-d", "--dataset")
    parser.add_argument("-t", "--table")
    parser.add_argument("-f", "--file")
    parser.add_argument("-c", "--date_columns")
    args = parser.parse_args()
    project = args.project
    dataset = args.dataset
    table = args.table
    file = args.file
    assert project is not None, "must provide --project"
    assert dataset is not None, "must provide --dataset"
    assert table is not None, "must provide --table"
    assert file is not None, "must provide --file"
    date_columns = str(args.date_columns).split(",") if args.date_columns else None
    BigQueryClientExecutor().import_data(project, dataset, table, file, date_columns)
