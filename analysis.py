import random
import duckdb
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import seaborn as sns
import pandas as pd
import numpy as np
import os
import datetime

DB_PATH = "nyt.duckdb"
TABLE_NAME = "articles"

# NYT filler words, don't want to see these as "most common" words
STOPWORDS = {
    "the", "and", "with", "for", "from", "this", "that", "into", "over", "about",
    "after", "before", "under", "through", "between", "during", "while",
    "where", "when", "which", "whose", "what", "who", "whom",
    "a", "an", "is", "are", "was", "were", "be", "been",
    "to", "of", "by", "on", "in", "at", "as",
    "it", "its", "they", "their", "them", "you", "your",
    "he", "his", "she", "her", "we", "our", "us",
    "not", "no", "yes", "but", "or", "if",
    "up", "down", "out", "off", "just", "more", "most", "all", "some",
    "new", "said", "says", "year", "years", "how", "one", "two", "three",
}


# tokenizer to process headline text
def simple_tokenize(text):
    if not text:
        return []

    # Lowercase
    text = text.lower()

    # Remove punctuation / numbers
    text = re.sub(r"[^a-z\s]", "", text)

    # Split into words
    words = text.split()

    # Remove short words + exclude "the"
    words = [w for w in words if len(w) > 2 and w not in STOPWORDS]

    return words


# main function that computes most common word per news desk
def most_common_word_per_desk():

    con = duckdb.connect(DB_PATH)
    print("Connected to DuckDB\n")

    # Pull just desk + headline
    df = con.execute(f"""
        SELECT news_desk, headline_main
        FROM {TABLE_NAME}
        WHERE news_desk IS NOT NULL
          AND headline_main IS NOT NULL
    """).fetchdf()

    con.close()

    print(f"Loaded {len(df):,} headlines\n")

    desk_word_counts = {}

    # Group headlines by desk
    for desk, group in df.groupby("news_desk"):

        # only desks with at least 1000 articles
        if len(group) < 1000:
            continue

        all_words = []

        for headline in group["headline_main"]:
            all_words.extend(simple_tokenize(headline))

        if all_words:
            desk_word_counts[desk] = Counter(all_words)

    # Print results
    print("MOST COMMON WORD PER DESK (≥ 1000 articles)")
    print("-" * 50)

    for desk, counter in desk_word_counts.items():
        word, count = counter.most_common(1)[0]
        print(f"{desk:20} - {word} ({count})")




def wordcloud_for_random_desks():

    con = duckdb.connect(DB_PATH)
    print("Connected to DuckDB\n")

    df = con.execute(f"""
        SELECT news_desk, headline_main
        FROM {TABLE_NAME}
        WHERE news_desk IS NOT NULL
          AND headline_main IS NOT NULL
    """).fetchdf()

    con.close()

    print(f"Loaded {len(df):,} headlines\n")

    desk_word_counts = {}

    # Build word counts per desk
    for desk, group in df.groupby("news_desk"):

        # Only desks with at least 1000 articles
        if len(group) < 1000:
            continue

        all_words = []

        for headline in group["headline_main"]:
            all_words.extend(simple_tokenize(headline))

        if all_words:
            desk_word_counts[desk] = Counter(all_words)

    # If not enough desks meet criteria
    if len(desk_word_counts) < 3:
        print("Not enough desks with ≥ 1000 articles for word cloud display.")
        return

    # Pick 3 desks randomly
    random_desks = random.sample(list(desk_word_counts.keys()), k=3)
    print(f"Generated word clouds for desks: {random_desks}\n")

    # Ensure output directory exists
    import os, datetime
    os.makedirs("plots", exist_ok=True)

    # Plot word clouds
    plt.figure(figsize=(18, 5))

    for i, desk in enumerate(random_desks, 1):

        wordcloud = WordCloud(
            width=600,
            height=400,
            background_color="white",
            colormap="tab10",
            max_words=60
        ).generate_from_frequencies(desk_word_counts[desk])

        plt.subplot(1, 3, i)
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")
        plt.title(desk)

    plt.suptitle("Word Clouds by Desk", fontsize=16)
    plt.tight_layout()

    # Save figure instead of showing
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"plots/wordcloud_{timestamp}.png"

    plt.savefig(filename, dpi=200)
    plt.close()

    print(f"Word cloud figure saved as {filename}")









def heatmap_top_desks_over_time():

    con = duckdb.connect(DB_PATH)
    print("Connected to DuckDB for heatmap\n")

    # sql - monthly article counts per desk
    df = con.execute(f"""
        SELECT
            news_desk,
            DATE_TRUNC('month', pub_date) AS month,
            COUNT(*) AS article_count
        FROM {TABLE_NAME}
        WHERE news_desk IS NOT NULL
          AND pub_date IS NOT NULL
        GROUP BY news_desk, month
    """).fetchdf()

    con.close()

    # identify top 10 desks by volume
    top_desks = (
        df.groupby("news_desk")["article_count"]
          .sum()
          .sort_values(ascending=False)
          .head(10)
          .index
    )

    filtered = df[df["news_desk"].isin(top_desks)]

    # pivot into heatmap matrix
    heatmap_matrix = filtered.pivot_table(
        index="news_desk",
        columns="month",
        values="article_count",
        fill_value=0
    )

    # sort months chronologically
    heatmap_matrix = heatmap_matrix.sort_index(axis=1)

    # apply log scale to handle wide range of counts
    log_heatmap = np.log1p(heatmap_matrix)

    # format month labels
    formatted_months = [
        pd.to_datetime(m).strftime("%B, %Y")
        for m in log_heatmap.columns
    ]
    log_heatmap.columns = formatted_months

    # plot heatmap
    plt.figure(figsize=(16, 6))

    sns.heatmap(
        log_heatmap,
        cmap="YlOrRd",
        linewidths=0.4,
        linecolor="gray",
        cbar_kws={"label": "Number of Articles Published That Month (Log Scale)"}
    )

    plt.title("Top 10 NYT Desks — Monthly Article Volume (Log Scale)", fontsize=14)
    plt.xlabel("Month")
    plt.ylabel("Desk")

    plt.xticks(rotation=45)

    plt.tight_layout()

    # save output
    os.makedirs("plots", exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = f"plots/desk_volume_heatmap_logscale_{timestamp}.png"
    plt.savefig(filename, dpi=200)
    plt.close()

    print(f"Heatmap saved to {filename}")


def avg_word_count_selected_desks_trend():

    # chosen desks to analyze
    TARGET_DESKS = [
        "politics",
        "sports",
        "business",
        "climate",
        "foreign",
        "national",
        "science",
        "arts&leisure"
    ]

    print("Connected to DuckDB for selected desk word count trend")

    con = duckdb.connect(DB_PATH)

    # avg word count per desk per month
    df = con.execute(f"""
        SELECT
            LOWER(news_desk) AS news_desk,
            DATE_TRUNC('month', pub_date) AS month,
            AVG(word_count) AS avg_word_count
        FROM {TABLE_NAME}
        WHERE news_desk IS NOT NULL
          AND pub_date IS NOT NULL
          AND word_count IS NOT NULL
        GROUP BY news_desk, month
    """).fetchdf()

    con.close()

    # filter to only target desks
    filtered = df[df["news_desk"].isin(TARGET_DESKS)].copy()

    if filtered.empty:
        print("No matching desks found. Check spelling or case sensitivity.")
        return

    # Convert month column cleanly
    filtered["month"] = pd.to_datetime(filtered["month"])

    # plot
    plt.figure(figsize=(14, 7))

    sns.lineplot(
        data=filtered,
        x="month",
        y="avg_word_count",
        hue="news_desk",
        linewidth=2
    )

    plt.title("Average Article Length Over Time — Selected NYT Desks", fontsize=14)
    plt.xlabel("Month")
    plt.ylabel("Average Word Count")
    plt.legend(title="Desk", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()

    # save output
    os.makedirs("plots", exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"plots/avg_word_count_selected_desks_{timestamp}.png"

    plt.savefig(filename, dpi=200)
    plt.close()

    print(f"Average word count trend saved to {filename}")



# run script
if __name__ == "__main__":
    most_common_word_per_desk()
    wordcloud_for_random_desks()
    heatmap_top_desks_over_time()
    avg_word_count_selected_desks_trend()