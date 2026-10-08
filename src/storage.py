import json

import boto3
from botocore.exceptions import ClientError


def get_s3_client(profile, region):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("s3")


def object_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_json(client, bucket, key, payload):
    body = json.dumps(payload).encode("utf-8")
    client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
