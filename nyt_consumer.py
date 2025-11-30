import json
import duckdb
import redis
from confluent_kafka import Consumer
from loguru import logger

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
db = duckdb.connect("storage/news.duckdb")

db.execute("""
CREATE TABLE IF NOT EXISTS articles (
    url TEXT,
    title TEXT,
    section TEXT,
    published TIMESTAMP,
    abstract TEXT
)
""")

consumer = Consumer({
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'nyt-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['nyt-articles'])

logger.info("Consumer started...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        logger.error(msg.error())
        continue

    article = json.loads(msg.value())

    url = article.get("url")
    title = article.get("title")
    section = article.get("section")
    abstract = article.get("abstract")
    published = article.get("published_date")

    db.execute("""
        INSERT INTO articles VALUES (?, ?, ?, ?, ?)
    """, [url, title, section, published, abstract])

    r.hincrby("section_counts", section or "unknown", 1)

    logger.info(f"Stored article: {title[:50]}")
