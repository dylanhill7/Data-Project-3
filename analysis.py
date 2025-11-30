import duckdb
import logging
import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def run_analysis():

    logging.info("Starting DuckDB analysis")
    con = duckdb.connect()

    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute(f"SET s3_region='{AWS_REGION}'")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}'")

    path = f"s3://{S3_BUCKET}/clean/articles/*.parquet"
    logging.info(f"Reading cleaned parquet: {path}")

    con.execute(f"""
        CREATE OR REPLACE TABLE articles AS
        SELECT *
        FROM read_parquet('{path}')
    """)

    # Monthly counts
    logging.info("Computing article count per month")
    monthly = con.execute("""
        SELECT
            strftime(CAST(pub_date AS DATE), '%Y-%m') AS month,
            COUNT(*) AS article_count
        FROM articles
        GROUP BY month
        ORDER BY month
    """).fetchdf()
    logging.info("\nMonthly Counts:\n%s", monthly)

    # Top sections
    logging.info("Computing top sections")
    sections = con.execute("""
        SELECT section_name, COUNT(*) AS count
        FROM articles
        GROUP BY section_name
        ORDER BY count DESC
        LIMIT 20
    """).fetchdf()
    logging.info("\nTop Sections:\n%s", sections)

    # Word count stats
    logging.info("Computing word count summary stats")
    wc = con.execute("""
        SELECT
            section_name,
            AVG(word_count) AS avg_words,
            MIN(word_count) AS min_words,
            MAX(word_count) AS max_words
        FROM articles
        WHERE word_count IS NOT NULL
        GROUP BY section_name
        ORDER BY avg_words DESC
        LIMIT 20
    """).fetchdf()
    logging.info("\nWord Count Stats:\n%s", wc)

    # ----------------------------------------------------
    # AUTHORS
    # ----------------------------------------------------

    logging.info("Computing top authors")

    con.execute("""
        UPDATE articles
        SET byline_original = NULL
        WHERE byline_original = '' OR byline_original = 'None'
    """)

    top_authors = con.execute("""
        SELECT
            byline_original AS author,
            COUNT(*) AS articles
        FROM articles
        WHERE byline_original IS NOT NULL
        GROUP BY author
        ORDER BY articles DESC
        LIMIT 20
    """).fetchdf()
    logging.info("\nTop Authors:\n%s", top_authors)

    logging.info("Computing most verbose authors")
    verbose = con.execute("""
        SELECT
            byline_original AS author,
            SUM(word_count) AS total_words,
            AVG(word_count) AS avg_words
        FROM articles
        WHERE byline_original IS NOT NULL
          AND word_count IS NOT NULL
        GROUP BY author
        ORDER BY total_words DESC
        LIMIT 20
    """).fetchdf()
    logging.info("\nMost Verbose Authors:\n%s", verbose)

    logging.info("Computing author monthly output trends")
    author_monthly = con.execute("""
        SELECT
            byline_original AS author,
            strftime(CAST(pub_date AS DATE), '%Y-%m') AS month,
            COUNT(*) AS article_count
        FROM articles
        WHERE byline_original IS NOT NULL
        GROUP BY author, month
        ORDER BY author, month
    """).fetchdf()

    logging.info("\nAuthor Productivity Over Time (first 20 rows):\n%s",
                 author_monthly.head(20))


if __name__ == "__main__":
    run_analysis()