import duckdb
import logging
import os
from dotenv import load_dotenv

# ----------------------------------------------------
# Load environment
# ----------------------------------------------------
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

CLEAN_PREFIX = "clean/articles"
TRANSFORM_PREFIX = "transform/articles_transformed.parquet"

# ----------------------------------------------------
# Logging
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ----------------------------------------------------
# Transform Pipeline
# ----------------------------------------------------
def run_transform():

    logging.info("Starting transform pipeline")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute(f"SET s3_region='{AWS_REGION}'")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}'")

    clean_path = f"s3://{S3_BUCKET}/{CLEAN_PREFIX}/*.parquet"
    out_path = f"s3://{S3_BUCKET}/{TRANSFORM_PREFIX}"

    logging.info(f"Reading cleaned data: {clean_path}")

    # Load into DuckDB temp table
    con.execute(f"""
        CREATE OR REPLACE TABLE base AS
        SELECT *
        FROM read_parquet('{clean_path}')
    """)

    logging.info("Loaded cleaned data")

    # ----------------------------------------------------
    # Normalize authors
    # ----------------------------------------------------
    con.execute("""
        UPDATE base
        SET byline_original = NULL
        WHERE byline_original = '' OR byline_original = 'None';
    """)

    # ----------------------------------------------------
    # Create normalized fields
    # ----------------------------------------------------
    logging.info("Applying transformations")

    con.execute("""
        CREATE OR REPLACE TABLE transformed AS
        SELECT
            id,
            CAST(pub_date AS TIMESTAMP) AS pub_timestamp,
            CAST(pub_date AS DATE) AS pub_date,
            strftime(CAST(pub_date AS DATE), '%Y-%m') AS pub_month,

            section_name,
            subsection_name,
            news_desk,
            type_of_material,
            document_type,

            byline_original AS author,

            word_count,
            CASE
                WHEN word_count < 300 THEN 'short'
                WHEN word_count BETWEEN 300 AND 1000 THEN 'medium'
                WHEN word_count BETWEEN 1001 AND 2000 THEN 'long'
                ELSE 'very_long'
            END AS word_category,

            headline_main,
            headline_print,
            web_url,
            snippet,
            lead_paragraph,
            source,

            keywords  -- still stringified list
        FROM base
    """)

    logging.info("Transformation step complete")

    # ----------------------------------------------------
    # Deduplicate
    # ----------------------------------------------------
    logging.info("Deduplicating by id")

    con.execute("""
        CREATE OR REPLACE TABLE deduped AS
        SELECT *
        FROM transformed
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY pub_date DESC) = 1
    """)

    # ----------------------------------------------------
    # Write back to S3
    # ----------------------------------------------------
    logging.info(f"Writing transformed data → {out_path}")

    con.execute(f"""
        COPY (SELECT * FROM deduped)
        TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    logging.info("Transform pipeline complete")


# ----------------------------------------------------
# Entry point
# ----------------------------------------------------
if __name__ == "__main__":
    run_transform()