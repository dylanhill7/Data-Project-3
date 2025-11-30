import requests
import os
import time
import json
from dotenv import load_dotenv
from quixstreams import Application
from confluent_kafka import Producer
from loguru import logger
from datetime import datetime, timedelta

load_dotenv()
API_KEY = os.getenv("NYT_API_KEY")

NYT_URL = "https://api.nytimes.com/svc/news/v3/content/all/all.json"

producer = Producer({'bootstrap.servers': 'localhost:19092'})

def fetch_articles(offset=0):
    params = {
        "api-key": API_KEY,
        "offset": offset
    }
    r = requests.get(NYT_URL, params=params)
    r.raise_for_status()
    return r.json().get("results", [])

def produce_article(article):
    producer.produce("nyt-articles", json.dumps(article).encode("utf-8"))

if __name__ == "__main__":
    logger.info("Starting NYT historical backfill...")
    
    for offset in range(0, 2000, 20):  # 2000 entries = fast scale
        articles = fetch_articles(offset)
        if not articles:
            break

        for article in articles:
            produce_article(article)

        logger.info(f"Ingested offset {offset}")
        time.sleep(0.5)

    producer.flush()
    logger.success("Backfill complete.")