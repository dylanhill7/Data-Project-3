import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
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

TRANSFORM_PREFIX = "transform/articles_transformed.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

sns.set(style="whitegrid")


# ----------------------------------------------------
# Utility: clean author column
# ----------------------------------------------------
def clean_author_column(df):
    # remove null or empty
    df = df[df.author.notna() & (df.author != "")]

    # institutional or desk-based bylines
    blacklist_exact = {
        "The New York Times",
        "By The New York Times",
        "By New York Times Games",
        "By The Learning Network",
        "By Wirecutter",
        "By Podcasts",
        "By Editorial Board",
        "By Magazine",
        "By Staff",
    }

    # remove exact matches
    df = df[~df.author.isin(blacklist_exact)]

    # remove anything that doesn't start with "By "
    df = df[df.author.str.startswith("By ")]

    # ensure real names have at least 2 name parts
    df = df[df.author.str.count(" ") >= 2]

    return df


# ----------------------------------------------------
# Load data
# ----------------------------------------------------
def load_data():
    logging.info("Reading transformed DuckDB dataset from S3")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{AWS_REGION}'")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}'")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}'")

    path = f"s3://{S3_BUCKET}/{TRANSFORM_PREFIX}"

    df = con.execute(f"""
        SELECT *
        FROM '{path}'
    """).fetchdf()

    # Ensure pub_month is sorted properly
    df["pub_month"] = pd.to_datetime(df["pub_month"])

    return df


# ----------------------------------------------------
# Visualization 1: Articles per desk over time
# ----------------------------------------------------
def plot_articles_per_desk(df):
    logging.info("Plotting articles per desk over time")

    desk_monthly = (
        df.groupby(["pub_month", "news_desk"])
          .size()
          .reset_index(name="article_count")
    )

    # Keep top 8 desks by total volume
    top_desks = (
        desk_monthly.groupby("news_desk")["article_count"]
        .sum()
        .sort_values(ascending=False)
        .head(8)
        .index
    )

    desk_monthly = desk_monthly[desk_monthly.news_desk.isin(top_desks)]

    plt.figure(figsize=(13, 7))
    sns.lineplot(
        data=desk_monthly,
        x="pub_month",
        y="article_count",
        hue="news_desk",
        marker="o"
    )
    plt.title("Articles Per Desk Over Time")
    plt.xlabel("Month")
    plt.ylabel("Article Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ----------------------------------------------------
# Visualization 2: Average word count per desk over time
# ----------------------------------------------------
def plot_avg_word_count(df):
    logging.info("Plotting average word count per desk over time")

    wc_monthly = (
        df.groupby(["pub_month", "news_desk"])["word_count"]
        .mean()
        .reset_index()
    )

    # Again restrict to top 8 desks
    top_desks = (
        df.groupby("news_desk")["word_count"]
        .count()
        .sort_values(ascending=False)
        .head(8)
        .index
    )

    wc_monthly = wc_monthly[wc_monthly.news_desk.isin(top_desks)]

    plt.figure(figsize=(13, 7))
    sns.lineplot(
        data=wc_monthly,
        x="pub_month",
        y="word_count",
        hue="news_desk",
        marker="o"
    )
    plt.title("Average Word Count Per Desk Over Time")
    plt.xlabel("Month")
    plt.ylabel("Average Words")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ----------------------------------------------------
# Visualization 3: Real human top authors over time
# ----------------------------------------------------
def plot_top_authors(df):
    logging.info("Plotting top authors over time")

    df = clean_author_column(df)

    # Count total output and pick top 10 authors
    top_authors = (
        df.groupby("author")
          .size()
          .sort_values(ascending=False)
          .head(10)
          .index
    )

    subset = df[df.author.isin(top_authors)]

    author_monthly = (
        subset.groupby(["pub_month", "author"])
        .size()
        .reset_index(name="article_count")
    )

    plt.figure(figsize=(13, 7))
    sns.lineplot(
        data=author_monthly,
        x="pub_month",
        y="article_count",
        hue="author",
        marker="o"
    )
    plt.title("Top 10 Real Human Authors Over Time")
    plt.xlabel("Month")
    plt.ylabel("Article Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# ----------------------------------------------------
# Main
# ----------------------------------------------------
def run_analysis():
    logging.info("Starting analysis pipeline")
    df = load_data()

    plot_articles_per_desk(df)
    plot_avg_word_count(df)
    plot_top_authors(df)

    logging.info("All analysis complete")


if __name__ == "__main__":
    run_analysis()