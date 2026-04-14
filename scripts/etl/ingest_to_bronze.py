"""
Glue Job: Ingestão NYC TLC Trip Data → S3 Bronze
Grava falhas no DynamoDB apenas após esgotar todas as tentativas de retry.
"""
import sys
import os
import time
import urllib.request
import urllib.error
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET", "DATASET", "DYNAMO_TABLE"])
s3 = boto3.client("s3")
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(args["DYNAMO_TABLE"])
bucket = args["TARGET_BUCKET"]
dataset = args["DATASET"]
prefix = dataset.split("_")[0]
BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

MAX_RETRIES = 5
MIN_FILE_SIZE = 1024

YEAR_RANGE = {
    "fhvhv_tripdata": (2019, 2025),
}
start_year, end_year = YEAR_RANGE.get(dataset, (2016, 2025))


def download(url, dest):
    """Tenta baixar com exponential backoff. Retorna (size, None) ou (None, error)."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            urllib.request.urlretrieve(url, dest)
            size = os.path.getsize(dest)
            if size < MIN_FILE_SIZE:
                raise ValueError(f"Arquivo muito pequeno: {size} bytes")
            return size, None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                # Arquivo não existe na fonte — não é falha, é skip
                return None, None
            last_error = str(e)
        except Exception as e:
            last_error = str(e)

        wait = 2 ** attempt
        print(f"  Tentativa {attempt + 1}/{MAX_RETRIES} falhou, aguardando {wait}s...")
        time.sleep(wait)

    # Esgotou todas as tentativas
    return None, last_error


for y in range(start_year, end_year + 1):
    for m in range(1, 13):
        f = f"{dataset}_{y}-{m:02d}.parquet"
        tmp = f"/tmp/{f}"
        s3_key = f"{prefix}/year={y}/month={m:02d}/{f}"
        url = f"{BASE}/{f}"

        print(f"Baixando: {url}")
        size, error = download(url, tmp)

        if size:
            s3.upload_file(tmp, bucket, s3_key)
            print(f"OK: {f} ({size / 1024 / 1024:.1f} MB)")
        elif error:
            print(f"FAIL após {MAX_RETRIES} tentativas: {f} - {error[:80]}")
            table.put_item(Item={
                "url": url,
                "dataset": dataset,
                "s3_key": s3_key,
                "error": error[:200],
            })
        else:
            print(f"SKIP: {f} (não existe na fonte)")

print(f"Ingestão {dataset} concluída.")
