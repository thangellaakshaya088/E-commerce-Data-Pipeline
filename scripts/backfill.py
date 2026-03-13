import os
import sys
import logging
import argparse
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill_pipeline(start_date: date, end_date: date) -> None:
    from src.processing.utils.spark_session import get_spark_session
    from src.processing.bronze_to_silver import run_all as bronze_to_silver
    from src.processing.session_builder import build_sessions
    from src.processing.silver_to_gold import run_all as silver_to_gold

    spark = get_spark_session("Backfill")
    current = start_date

    while current <= end_date:
        logger.info(f"Backfilling {current}")
        try:
            bronze_to_silver(spark)
            build_sessions(spark)
            silver_to_gold(spark)
            logger.info(f"Completed backfill for {current}")
        except Exception as e:
            logger.error(f"Failed backfill for {current}: {e}")
        current += timedelta(days=1)

    spark.stop()
    logger.info("Backfill complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    backfill_pipeline(start, end)
