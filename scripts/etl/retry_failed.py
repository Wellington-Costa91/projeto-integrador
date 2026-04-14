"""
Glue Job: Retry de downloads que falharam (lê do DynamoDB, tenta novamente, apaga se OK)
"""
import sys
import os
import time
import urllib.request
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET", "DYNAMO_TABLE"])
s3 = boto3.client("s3")
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(args["DYNAMO_TABLE"])
bucket = args["TARGET_BUCKET"]

MAX_RETRIES = 5
MIN_FILE_SIZE = 1024


def download_with_backoff(url, dest):
    for attempt in range(MAX_RETRIES):
        try:
            urllib.request.urlretrieve(url, dest)
            size = os.path.getsize(dest)
            if size < MIN_FILE_SIZE:
                raise ValueError(f"Arquivo muito pequeno: {size} bytes")
            return size
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 2 ** attempt
            print(f"  Retry {attempt + 1}/{MAX_RETRIES} em {wait}s - {str(e)[:80]}")
            time.sleep(wait)


# Lê todos os itens com falha
response = table.scan()
items = response.get("Items", [])

while "LastEvaluatedKey" in response:
    response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
    items.extend(response.get("Items", []))

print(f"Total de downloads pendentes: {len(items)}")

for item in items:
    url = item["url"]
    s3_key = item["s3_key"]
    f = url.split("/")[-1]
    tmp = f"/tmp/{f}"

    try:
        print(f"Retry: {url}")
        size = download_with_backoff(url, tmp)
        s3.upload_file(tmp, bucket, s3_key)
        table.delete_item(Key={"url": url})
        print(f"OK: {f} ({size / 1024 / 1024:.1f} MB) - removido do DynamoDB")
    except Exception as e:
        print(f"FAIL novamente: {f} - {str(e)[:80]}")

print("Retry concluído.")
