import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean_and_transform.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "nyt.duckdb"
TABLE_NAME = "articles"

# cleaning steps: check missing values, get rid of duplicates, switch date time format

def check_missing_values():

    con = None

    try:
        # connect to duckdb
        con = duckdb.connect(database=DB_PATH, read_only=False)
        print("Connected to DuckDB for missing value check\n")

        # fetch column names
        columns = [
            row[0] for row in con.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{TABLE_NAME}'
            """).fetchall()
        ]

        # count missing values
        total_missing = con.execute(f"""
            SELECT SUM(
                {" + ".join([f"CASE WHEN {col} IS NULL THEN 1 ELSE 0 END" for col in columns])}
            )
            FROM {TABLE_NAME}
        """).fetchone()[0]

        # provide initial report
        if total_missing > 0:
            print(f"Missing values detected: {total_missing:,}\n")
        else:
            print("No missing values detected\n")
            return

        print("Applying cleaning policy:\n")
        print("• Drop column if > 10% missing")
        print("• Otherwise drop rows containing missing values\n")

        # cleaning loop
        for col in columns:

            missing = con.execute(f"""
                SELECT COUNT(*)
                FROM {TABLE_NAME}
                WHERE {col} IS NULL
            """).fetchone()[0]

            if missing == 0:
                continue

            pct = missing / total_rows

            # drop column (if >10% missing)
            if pct > 0.10:
                print(f"Dropping column '{col}' ({missing:,} missing = {round(pct*100,2)}%)")
                con.execute(f"ALTER TABLE {TABLE_NAME} DROP COLUMN {col}")

            # drop rows (if <=10% missing)
            else:
                print(f"Dropping {missing:,} rows where '{col}' is NULL ({round(pct*100,2)}%)")
                con.execute(f"""
                    DELETE FROM {TABLE_NAME}
                    WHERE {col} IS NULL
                """)

                # update row count
                total_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

        # refresh columns after dropping
        remaining_columns = [
            row[0] for row in con.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{TABLE_NAME}'
            """).fetchall()
        ]

        # final validation
        remaining_missing = con.execute(f"""
            SELECT SUM(
                {" + ".join([f"CASE WHEN {col} IS NULL THEN 1 ELSE 0 END"
                             for col in remaining_columns])}
            )
            FROM {TABLE_NAME}
        """).fetchone()[0]

        # print final report
        print("\nFinal Result")
        print("-" * 50)

        if remaining_missing == 0:
            print("All missing values resolved")
            logger.info("All missing values resolved")
        else:
            print(f"Missing values remain: {remaining_missing:,}")
            logger.warning(f"Remaining missing values: {remaining_missing}")

    except Exception as e:
        print(f"ERROR: {e}")
        logger.error(f"Cleaning script error: {e}")

    finally:
        if con:
            con.close()
            print("\nDuckDB connection closed")
            logger.info("DuckDB connection closed")


def remove_duplicates():

    con = None

    try:
        # connect to duckdb
        con = duckdb.connect(database=DB_PATH, read_only=False)
        print("\nConnected to DuckDB for duplicate check\n")

        # initial total rows
        before = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

        # counting duplicate IDs
        dup_count = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT id
                FROM {TABLE_NAME}
                GROUP BY id
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        if dup_count == 0:
            print("No duplicate article IDs detected\n")
            return

        print(f"{dup_count:,} duplicate article IDs found\n")
        print("Removing duplicates (keeping one copy per article)")

        # remove duplicates
        con.execute(f"""
            DELETE FROM {TABLE_NAME}
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY id ORDER BY pub_date DESC) AS rn
                    FROM {TABLE_NAME}
                )
                WHERE rn > 1
            )
        """)

        # total rows after
        after = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        removed = before - after

        print(f"Removed {removed:,} total duplicates!")
        print(f"Rows before: {before:,}")
        print(f"Rows after:  {after:,}\n")

    except Exception as e:
        print(f"ERROR removing duplicates: {e}")

    finally:
        if con:
            con.close()
            print("DuckDB connection closed\n")


def convert_pub_date():

    con = None
    try:
        con = duckdb.connect(DB_PATH, read_only=False)
        print("Connected to DuckDB for date conversion")

        con.execute("""
            ALTER TABLE articles
            ALTER COLUMN pub_date TYPE TIMESTAMP;
        """)

        print("pub_date converted to timestamp successfully\n")

    except Exception as e:
        print(f"ERROR converting dates: {e}")

    finally:
        if con:
            con.close()
            print("DuckDB connection closed")


if __name__ == "__main__":
    check_missing_values()
    remove_duplicates()
    convert_pub_date()