import os
import base64
import json
import logging
from aws_kinesis_agg.deaggregator import deaggregate_records
from src.consumers.mysql_consumer import MySQLConsumer

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "WARN"))


def handle_event(event, context):
    raw_records = event["Records"]
    records = deaggregate_records(raw_records)
    mysql = None
    i = 0
    for record in records:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode()) # noqa
        if mysql is None:
            mysql = MySQLConsumer(payload["database"])
        mysql.process_row(payload)
        i = i + 1

    logger.warn("Number of records processed: {} ".format(str(i)))
    mysql.close()
