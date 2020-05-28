import os
import base64
import json
import logging
from aws_kinesis_agg.deaggregator import deaggregate_records
from src.db.mysql_connection import MySQLConnector

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "WARN"))


def handle_event(event, context):
    raw_records = event["Records"]
    records = deaggregate_records(raw_records)
    mysql = None
    for record in records:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]).decode()) # noqa
        if mysql is None:
            mysql = MySQLConnector(payload["database"])
        mysql.process_row(payload)
