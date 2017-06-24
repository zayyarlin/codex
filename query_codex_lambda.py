import psycopg2

import sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = psycopg2.connect(database='', user='', password="",
                            host='', port='')
    cur = conn.cursor()
except:
    logger.error("ERROR: Unexpected error: Could not connect to Postgres instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS postgres instance succeeded")


def handler(event, context):
    """
    This function fetches content from mysql RDS instance
    """
    item_count = 0
    with conn.cursor() as cur:
        cur.execute("select * from package")
        for row in cur:
            item_count += 1
            logger.info(row)

    return "selected %d items from RDS postgres table" % (item_count)