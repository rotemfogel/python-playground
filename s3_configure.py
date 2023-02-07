import os

import boto3

if __name__ == "__main__":
    region_name = os.getenv("REGION")
    bucket = os.getenv("BUCKET")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_client = boto3.Session(
        # aws_access_key_id=aws_access_key_id,
        # aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        profile_name="prod",
    ).client("s3", region_name=region_name)
    bucket_notifications = s3_client.get_bucket_notification(Bucket=bucket)
    print(bucket_notifications)
