from prefect import flow, task, get_run_logger
from datetime import datetime
import requests
import boto3
import json
import os
import fsspec
import time
from dotenv import load_dotenv
load_dotenv()

# configuration
NYT_API_KEY = os.getenv("NYT_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
BASE_URL = "https://api.nytimes.com/svc/archive/v1"
MANIFEST_PATH = "manifest/nyt_manifest.json"
s3 = boto3.client("s3")


# -------------------------- task 1 - rate limiting policy --------------------------
# api allows 5 calls per minute, thus rate limiting policy should be to wait 12 seconds between calls

@task
def apply_rate_limit(seconds: int = 12):
    logger = get_run_logger()
    logger.info(f"Applying rate limit sleep: {seconds} seconds")
    time.sleep(seconds)




# ------------------------- task 2 - fetch data from NYT API -------------------------
# 3 retries with 10 second delay between retries, for case of failed requests
# wait time between retries is different than rate limiting time

@task(retries=3, retry_delay_seconds=10)
def fetch_archive_month(year: int, month: int) -> dict: # task is fetching NYT archive for given year/month, returns payload as dict
    logger = get_run_logger()

    # quickly stops execution if api key is not set
    if not NYT_API_KEY:
        raise ValueError("NYT_API_KEY not set")

    # construct url with year/month and api key as param, log which year/month archive is being fetched
    url = f"{BASE_URL}/{year}/{month}.json"
    params = {"api-key": NYT_API_KEY}
    logger.info(f"Fetching archive: {year}-{month:02d}")

    response = requests.get(url, params=params, timeout=60) # merging beginning of url with params/api key to get full url call

    # if API call fails, log the error with the response code and the url with the month/year, raise the error to trigger retry
    if response.status_code != 200:
        logger.error(f"Fetch failed | Status: {response.status_code} | URL: {url}")
        response.raise_for_status()

    payload = response.json()

    # count and log number of articles fetched
    count = len(payload.get("response", {}).get("docs", []))
    logger.info(f"Fetched {count} articles")

    return payload



# ------------------------- task 3 - validate payload -------------------------
# ensure payload has expected structure and is not empty

@task
def validate_payload(payload: dict) -> dict:
    logger = get_run_logger()

    if "response" not in payload:
        raise ValueError("Missing 'response' field")

    if "docs" not in payload["response"]:
        raise ValueError("Missing 'docs' field")

    article_count = len(payload["response"]["docs"])

    if article_count == 0:
        raise ValueError("Empty payload — no articles returned")

    logger.info(f"Payload validated: {article_count} articles")

    return payload





# ------------------------- task 4 - upload to S3 -------------------------
# upload validated payload to s3 bucket in structured


@task
def upload_to_s3(year: int, month: int, payload: dict):
    logger = get_run_logger()

    key = f"raw/nyt_archive/{year}/{month:02d}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

    logger.info(f"Uploaded → s3://{S3_BUCKET}/{key}")
    return key


# -------------------------
# TASK 4 — LOAD MANIFEST
# -------------------------
@task
def load_manifest():
    logger = get_run_logger()

    fs = fsspec.filesystem("s3")

    if not fs.exists(f"s3://{S3_BUCKET}/{MANIFEST_PATH}"):
        logger.info("No manifest found — creating new one")
        return {"completed": {}, "success": 0, "failed": 0}

    with fs.open(f"s3://{S3_BUCKET}/{MANIFEST_PATH}") as f:
        manifest = json.load(f)

    return manifest


# -------------------------
# TASK 5 — UPDATE MANIFEST
# -------------------------
@task
def update_manifest(manifest: dict, year: int, month: int, success: bool):
    logger = get_run_logger()

    key = f"{year}-{month:02d}"

    if "completed" not in manifest:
        manifest["completed"] = {}

    manifest["completed"][key] = "success" if success else "failed"

    if success:
        manifest["success"] += 1
    else:
        manifest["failed"] += 1

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=MANIFEST_PATH,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )

    logger.info(f"Manifest updated: {key} -> {'success' if success else 'failed'}")
    return manifest


# -------------------------
# FLOW — MAIN PIPELINE
# -------------------------
@flow
def nyt_archive_flow(start_year=2022, end_year=2024, rate_limit_seconds=12):
    logger = get_run_logger()
    logger.info("Starting NYT Archive ingestion pipeline")

    manifest = load_manifest()

    now = datetime.utcnow()

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):

            # Skip future months
            if year == now.year and month > now.month:
                break

            key = f"{year}-{month:02d}"

            # Skip already ingested
            if key in manifest["completed"]:
                logger.info(f"Skipping already ingested: {key}")
                continue

            try:
                apply_rate_limit(rate_limit_seconds)

                payload = fetch_archive_month(year, month)
                payload = validate_payload(payload)

                upload_to_s3(year, month, payload)

                manifest = update_manifest(manifest, year, month, success=True)

            except Exception as e:
                logger.error(f"FAILED month {key}: {e}")
                manifest = update_manifest(manifest, year, month, success=False)


    logger.info("Pipeline finished ✅")


if __name__ == "__main__":
    nyt_archive_flow(start_year=2022, end_year=2024)

