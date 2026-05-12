"""
Glue Job: Ingestão NYC TLC Trip Data → S3 Bronze
- Download paralelo com ThreadPool (10 threads)
- Skip de arquivos já existentes no S3
- Exponential backoff em falhas
- Falhas registradas no DynamoDB
"""
import sys
import os
import time
import urllib.request
import urllib.error
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
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
MAX_WORKERS = 10

# Ranges reais de disponibilidade por dataset (fonte: NYC TLC)
DATASET_RANGES = {
    "yellow_tripdata": (2016, 2026),
    "green_tripdata":  (2016, 2026),
    "fhvhv_tripdata":  (2019, 2026),  # começa em fev/2019
    "fhv_tripdata":    (2016, 2026),
}
# Meses iniciais para datasets que não começam em janeiro
DATASET_START_MONTH = {
    "fhvhv_tripdata": (2019, 2),   # fev/2019
}

start_year, end_year = DATASET_RANGES.get(dataset, (2016, 2026))


def s3_key_exists(s3_key):
    """Verifica se o objeto já existe no S3."""
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        return True
    except s3.exceptions.ClientError:
        return False


def download(url, dest):
    """Baixa com exponential backoff. Retorna (size, None) | (None, None=skip) | (None, error)."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            urllib.request.urlretrieve(url, dest)
            size = os.path.getsize(dest)
            if size < MIN_FILE_SIZE:
                raise ValueError(f"Arquivo muito pequeno: {size} bytes")
            return size, None
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None, None
            last_error = str(e)
        except Exception as e:
            last_error = str(e)
        time.sleep(2 ** attempt)
    return None, last_error


def process_file(y, m):
    """Baixa e envia um arquivo para o S3. Retorna (status, filename, detail)."""
    f = f"{dataset}_{y}-{m:02d}.parquet"
    s3_key = f"{prefix}/year={y}/month={m:02d}/{f}"

    # Skip se já existe
    if s3_key_exists(s3_key):
        return "SKIP_EXISTS", f, None

    url = f"{BASE}/{f}"
    tmp = f"/tmp/{f}"

    size, error = download(url, tmp)

    if size:
        s3.upload_file(tmp, bucket, s3_key)
        try:
            os.remove(tmp)
        except OSError:
            pass
        return "OK", f, size
    elif error:
        table.put_item(Item={
            "url": url, "dataset": dataset,
            "s3_key": s3_key, "error": error[:200],
        })
        return "FAIL", f, error
    else:
        return "SKIP_404", f, None


# Monta lista de tarefas respeitando o range real do dataset
tasks = []
start_y_m = DATASET_START_MONTH.get(dataset)
for y in range(start_year, end_year + 1):
    for m in range(1, 13):
        if start_y_m and (y < start_y_m[0] or (y == start_y_m[0] and m < start_y_m[1])):
            continue
        tasks.append((y, m))

ok = skip_exists = skip_404 = fail = 0
total_bytes = 0

print(f"Iniciando ingestao {dataset}: {len(tasks)} arquivos, {MAX_WORKERS} threads")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_file, y, m): (y, m) for y, m in tasks}

    for future in as_completed(futures):
        status, f, detail = future.result()
        if status == "OK":
            ok += 1
            total_bytes += detail
            print(f"OK: {f} ({detail / 1024 / 1024:.1f} MB)")
        elif status == "SKIP_EXISTS":
            skip_exists += 1
        elif status == "SKIP_404":
            skip_404 += 1
        elif status == "FAIL":
            fail += 1
            print(f"FAIL: {f} - {str(detail)[:80]}")

print(f"\nIngestao {dataset} concluida:")
print(f"  OK={ok} ({total_bytes / 1024 / 1024 / 1024:.1f} GB) | "
      f"Ja existiam={skip_exists} | Nao encontrados={skip_404} | Falhas={fail}")
