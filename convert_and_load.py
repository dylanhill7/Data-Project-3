import os
import duckdb
import ijson
import logging
import traceback
import pandas as pd
import s3fs
from dotenv import load_dotenv
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


# --------------------------------- converting NYT JSON files in S3 to Parquet files ---------------------------------

# defining where raw JSON data is stored and where clean parquet data will be written
RAW_PREFIX = "raw/nyt_archive"
CLEAN_PREFIX = "clean/articles"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    filename="convert_and_load.log"
)

# creates a file-system-style connection to S3 so Python can read and write objects as if they were local files
s3_fs = s3fs.S3FileSystem(
    key=AWS_ACCESS_KEY,
    secret=AWS_SECRET_KEY,
    client_kwargs={"region_name": AWS_REGION},
)

# will take nested json object and pulls values out into a single flat dictionary with what we believe to be the most important
# column names as keys, allows it to be converted to parquet
def flatten_doc(doc):
    headline = doc.get("headline") or {}
    byline = doc.get("byline") or {}

    return {
        # Core identifiers
        "id": doc.get("_id"),
        "web_url": doc.get("web_url"),
        "source": doc.get("source"),
        "document_type": doc.get("document_type"),
        "type_of_material": doc.get("type_of_material"),

        # Time
        "pub_date": doc.get("pub_date"),

        # Editorial structure
        "news_desk": doc.get("news_desk"),
        "section_name": doc.get("section_name"),
        "subsection_name": doc.get("subsection_name"),

        # Headlines
        "headline_main": headline.get("main"),
        "headline_print": headline.get("print_headline"),
        "headline_seo": headline.get("seo"),

        # Content
        "snippet": doc.get("snippet"),
        "lead_paragraph": doc.get("lead_paragraph"),
        "word_count": doc.get("word_count"),

        # Attribution
        "byline_original": byline.get("original"),

        # Tags
        "keywords_raw": doc.get("keywords"),
        "keywords": str(doc.get("keywords", [])),

        # Print prominence (added)
        "print_page": doc.get("print_page"),
        "print_section": doc.get("print_section"),

        # Media richness (added)
        "multimedia_count": len(doc.get("multimedia", [])) if doc.get("multimedia") else 0,

        # Language (optional but useful)
        "language": doc.get("language"),
    }


# actually does the processing of a single month file - reads json from s3, flattens each article, writes parquet back to s3
def process_month(key: str):

    logging.info(f"Processing {key}")


    try:
        # building s3 path to one month of NYT data
        s3_path = f"s3://{S3_BUCKET}/{key}"

        # creating list that will hold cleaned rows, streams a huge NYT JSON file from S3, extracts each article one-by-one, 
        # flattens it into a clean row, and stores it in memory for conversion into a table
        rows = []
        with s3_fs.open(s3_path, "rb") as f:
            parser = ijson.items(f, "response.docs.item")
            for item in parser:
                rows.append(flatten_doc(item))

        # creates a pandas DataFrame from newly cleaned list of rows
        df = pd.DataFrame(rows)

        # writes the cleaned DataFrame back to S3 as a parquet file with appropriate path and year/month filename
        year = key.split("/")[-2]
        month = key.split("/")[-1].replace(".json", "")
        out_key = f"{CLEAN_PREFIX}/{year}-{month}.parquet"
        out_path = f"s3://{S3_BUCKET}/{out_key}"

        with s3_fs.open(out_path, "wb") as f:
            df.to_parquet(f, index=False)

        logging.info(f"Wrote cleaned file → {out_path}")
        return True

    except Exception as e:
        logging.error(f"Failed processing {key}: {e}")
        logging.error(traceback.format_exc())
        return False


# uses process month function to run through all raw files/months of data in S3 and process them one-by-one
def run_cleaning():
    logging.info("Starting cleaning pipeline")

    keys = s3_fs.glob(f"{S3_BUCKET}/{RAW_PREFIX}/*/*.json")
    if not keys:
        logging.error("No JSON files found in S3")
        return

    logging.info(f"Found {len(keys)} raw files")

    success = fail = 0

    for full_path in keys:
        key = full_path.replace(f"{S3_BUCKET}/", "")
        ok = process_month(key)
        success += ok
        fail += 1 - ok

    logging.info(f"Cleaning finished. Success: {success}, Fail: {fail}")



# --------------------------------- load parquet from S3 into duckDB ---------------------------------

DB_PATH = "nyt.duckdb"
PARQUET_PATH = f"s3://{S3_BUCKET}/clean/articles/*.parquet"


def table_exists(con, table_name):
    result = con.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
    """).fetchone()[0]
    return result > 0


def load_parquet_into_duckdb():
    con = None

    try:
        logging.info("Starting DuckDB load job from S3")

        # make connection to persistent duckdb database file
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logging.info("Connected to DuckDB database")
        print("Connected to DuckDB")

        # load httpfs extension for S3 access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # configuring aws credentials in duckdb
        con.execute(f"""
            SET s3_region='{AWS_REGION}';
            SET s3_access_key_id='{AWS_ACCESS_KEY}';
            SET s3_secret_access_key='{AWS_SECRET_KEY}';
        """)
        logging.info("DuckDB configured for S3 access")

        # check if articles table already exists, if not create it by reading parquet files from s3
        table_name = "articles"

        # ALWAYS DROP AND REBUILD TABLE
        logging.info(f"Dropping table '{table_name}' if it exists")

        con.execute(f"DROP TABLE IF EXISTS {table_name}")

        logging.info(f"Rebuilding table '{table_name}' from S3 parquet files")

        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT *
            FROM parquet_scan('{PARQUET_PATH}')
        """)

        logging.info("Articles table recreated from S3 parquet files")
        print("Rebuilt DuckDB table from S3")

        # fetching row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logging.info(f"{table_name} row count: {count:,}")
        print(f"Total rows in DuckDB: {count:,}")

        # generating some basic stats
        stats = con.execute(f"""
            SELECT
                COUNT(*) AS total_articles,
                AVG(word_count) AS avg_words,
                MEDIAN(word_count) AS median_words,
                MAX(word_count) AS max_words
            FROM {table_name}
        """).fetchone()

        total, avg_wc, med_wc, max_wc = stats

        summary = (
            f"\nSummary Stats:\n"
            f"Total Articles: {total:,}\n"
            f"Average Word Count: {round(avg_wc)}\n"
            f"Median Word Count: {round(med_wc)}\n"
            f"Max Word Count: {max_wc}\n"
        )

        print(summary)
        logging.info(summary)

    except Exception as e:
        logging.error(f"DUCKDB LOAD FAILED: {e}")
        print(f"Error loading DuckDB: {e}")

    finally:
        if con:
            con.close()
            logging.info("DuckDB connection closed")
            print("DuckDB connection closed")

if __name__ == "__main__":
    run_cleaning()
    load_parquet_into_duckdb()