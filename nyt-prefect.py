NYT_API_KEY = os.getenv("NYT_API_KEY")
BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
DB_PATH = "storage/nyt.duckdb"


from prefect import task, get_run_logger
import requests
import os

NYT_API_KEY = os.getenv("NYT_API_KEY")
BASE_URL = "https://api.nytimes.com/svc/archive/v1"


# task 1 - fetch data from NYT API

@task
def fetch_month(year, month):


from prefect import task, get_run_logger
import requests
import os

NYT_API_KEY = os.getenv("NYT_API_KEY")
BASE_URL = "https://api.nytimes.com/svc/archive/v1"


@task(retries=3, retry_delay_seconds=10)
def fetch_archive_month(year: int, month: int) -> dict:
    logger = get_run_logger()

    if not NYT_API_KEY:
        raise ValueError("NYT_API_KEY not set. Put it in your .env file.")

    url = f"{BASE_URL}/{year}/{month}.json"
    params = {"api-key": NYT_API_KEY}

    logger.info(f"Fetching NYT archive for {year}-{month:02d}")

    response = requests.get(url, params=params, timeout=60)

    if response.status_code != 200:
        logger.error(
            f"Request failed | Status {response.status_code} | URL: {response.url}"
        )
        response.raise_for_status()

    payload = response.json()

    article_count = len(payload.get("response", {}).get("docs", []))
    logger.info(f"Retrieved {article_count} articles")

    return payload














# FIRST TASK: Sends a POST request to populate the SQS queue with messages


# -------------------------
# TASK 1 — Fetch one page
# -------------------------
@task(retries=3, retry_delay_seconds=10)
def fetch_page(query, begin_date, end_date, page):
    logger = get_run_logger()

    params = {
        "q": query,
        "begin_date": begin_date,
        "end_date": end_date,
        "page": page,
        "api-key": NYT_API_KEY
    }

    logger.info(f"Fetching: q={query}, page={page}, window={begin_date}-{end_date}")

    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    return data["response"]["docs"]


# -------------------------
# TASK 2 — Ensure DB exists
# -------------------------
@task
def initialize_duckdb():
    con = duckdb.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY,
            headline TEXT,
            section TEXT,
            pub_date TIMESTAMP,
            abstract TEXT,
            word_count INTEGER,
            url TEXT
        )
    """)
    con.close()


# -------------------------
# TASK 3 — Insert records
# -------------------------
@task
def insert_articles(articles):
    logger = get_run_logger()
    con = duckdb.connect(DB_PATH)

    count = 0
    for a in articles:
        try:
            doc_id = a.get("_id")
            headline = a.get("headline", {}).get("main")
            section = a.get("section_name")
            pub_date = a.get("pub_date")
            abstract = a.get("abstract")
            word_count = a.get("word_count")
            url = a.get("web_url")

            con.execute("""
                INSERT OR IGNORE INTO articles
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [doc_id, headline, section, pub_date, abstract, word_count, url])

            count += 1
        except Exception as e:
            logger.warning(f"Insert failed: {e}")

    logger.info(f"Inserted {count} articles")
    con.close()
    return count


# -------------------------
# FLOW — Main ingestion job
# -------------------------
@flow
def nyt_ingestion_flow():

    logger = get_run_logger()
    logger.info("Starting NYT Ingestion Flow")

    initialize_duckdb()

    queries = [
        "politics", "economy", "climate", "technology", "health",
        "election", "artificial intelligence", "war", "china", "covid"
    ]

    start = datetime(2022, 1, 1)
    end = datetime(2023, 12, 31)
    window = timedelta(days=7)

    total_inserted = 0

    while start < end:
        begin_date = start.strftime("%Y%m%d")
        end_date = (start + window).strftime("%Y%m%d")

        logger.info(f"Processing time window {begin_date} → {end_date}")

        for query in queries:
            for page in range(100):
                try:
                    docs = fetch_page(query, begin_date, end_date, page)

                    if not docs:
                        break

                    count = insert_articles(docs)
                    total_inserted += count

                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Fetch error: {e}")
                    time.sleep(10)

        start += window

    logger.info(f"✅ Ingestion Complete. Total inserted: {total_inserted}")


if __name__ == "__main__":
    nyt_ingestion_flow()