import os

from dotenv import load_dotenv

from airfart.operators.postgres.postgres_to_s3 import PostgresToS3Operator

if __name__ == "__main__":
    load_dotenv()
    w = PostgresToS3Operator(
        sql="SELECT * FROM public.earnings_data_export",
        bucket=os.getenv("DATA_BUCKET"),
        database=os.getenv("DATABASE"),
        db_conn_id="stats",
        post_db_path="dba/rotem/earnings_data_export",
        file_name="full_dump",
    )
    w.execute()
