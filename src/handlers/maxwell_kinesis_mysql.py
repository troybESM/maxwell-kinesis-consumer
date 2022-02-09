import os
import base64
import json
import logging
from aws_kinesis_agg.deaggregator import deaggregate_records
from src.consumers.mysql_consumer import MySQLConsumer
from src.utils import get_secret

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def handle_event(event, context):
    raw_records = event["Records"]
    records = deaggregate_records(raw_records)
    mysql = None
    secret_string = None
    i = 0

    for record in records:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode()) # noqa
        if secret_string is None:
            try:
                secret_string = get_secret(f"/maxwell/{os.environ.get('CLUSTER_NAME')}") # noqa
            except Exception:
                logger.warn(f"No secret found for table, ignoring. Cluster: /maxwell/{os.environ.get('CLUSTER_NAME')}")
                return

        if mysql is None:
            mysql = MySQLConsumer(payload["database"], secret_string)
            logger.info("Processing records for: {}".format(payload["database"])) # noqa
        mysql.process_row(payload)
        i = i + 1

    logger.info("Number of records processed: {} ".format(str(i)))
    mysql.close()
