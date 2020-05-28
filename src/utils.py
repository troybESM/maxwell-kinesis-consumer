import os
import json
import logging
import functools
from boto3.session import Session
from botocore.exceptions import ClientError
from boto3_type_annotations.secretsmanager import Client

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "WARN"))


@functools.lru_cache(maxsize=32)
def get_secret(secret_name: str) -> dict:
    secret_client: Client = Session().client("secretsmanager")
    try:
        secret_value: dict = secret_client.get_secret_value(SecretId=secret_name) # noqa E501
        if isinstance(secret_value.get("SecretString"), str):
            return json.loads(secret_value.get("SecretString"))
        else:
            return secret_value.get("SecretString")
    except ClientError as e:
        logger.error(json.dumps(e.response))
        raise
