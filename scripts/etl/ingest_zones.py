"""
Glue Job: Ingestão taxi_zone_lookup.csv → S3 Bronze
"""
import sys
import urllib.request
import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET"])
s3 = boto3.client("s3")

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TMP = "/tmp/taxi_zone_lookup.csv"
S3_KEY = "zones/taxi_zone_lookup.csv"

print(f"Baixando: {URL}")
urllib.request.urlretrieve(URL, TMP)
s3.upload_file(TMP, args["TARGET_BUCKET"], S3_KEY)
print(f"OK: s3://{args['TARGET_BUCKET']}/{S3_KEY}")
