import json
import logging
from pymysql import connect, IntegrityError, ProgrammingError, escape_string
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from src.utils import get_secret

logger = logging.getLogger()
logger.setLevel("DEBUG")


class MySQLConnector:
    def __init__(self, db_name):
        secret_string = get_secret("fmgmt-c1-maxwell")
        self.database_name = db_name
        self.connection: Connection = connect(
            host="px-fmgmt-c1.proxy-cq8zuscratld.us-west-2.rds.amazonaws.com",
            user=secret_string["username"],
            passwd=secret_string["password"],
            port=secret_string["port"],
            autocommit=True,
            db=db_name
        )
        self.cursor: Cursor = self.connection.cursor(Cursor)

    def process_row(self, data: dict) -> None:
        logger.debug("process_row, database = {}".format(data["database"]))
        sql = ""
        if data["type"] == "insert" or data["type"] == "bootstrap-insert":
            sql = self.__gen_insert_sql(data)
            self.__execute_query(sql)
        elif data["type"] == "update":
            sql = self.__gen_update_sql(data)
            self.__execute_query(sql)
        else:
            logger.error("Unsupported DDL/DML operation: {}".format(data["type"])) # noqa
            logger.error("data dict for unsupported operation:  {}".format(json.dumps(data))) # noqa

    def __execute_query(self, sql: str) -> None:
        logger.debug("__execute query sql: {} ".format(sql))
        try:
            self.cursor.execute(sql)
        except (IntegrityError, ProgrammingError) as error:
            logger.error("Integrity/Programming error SQL: {}".format(sql))
            logger.error(error)
            # send to DLQ ?
        except Exception as e:
            logger.error("Other error SQL: {}".format(sql))
            logger.error(e)
            raise

    def __gen_insert_sql(self, record: dict) -> str:
        table_name = record["table"]
        sql = "INSERT INTO {} ( {} ) VALUES ( {} )".format(table_name,
                                                self.gen_insert_col_list(record), # noqa
                                                self.gen_insert_value_list(record)) # noqa
        logger.debug("Generated SQL for insert: {}".format(sql))
        return sql

    def __gen_update_sql(self, record: dict) -> str:
        table_name = record["table"]
        set_values = list()
        where_values = list()

        for k, v in record["data"].items():
            if k not in record["primary_key_columns"]:
                if v is None or v == "NULL":
                    set_values.append("`" + k + "`" + " = NULL") # noqa
                elif isinstance(v, dict):
                    set_values.append("'" + escape_string(json.dumps(v)) + "'")
                elif isinstance(v, str):
                    set_values.append("`" + k + "`" + " = '" + escape_string(v) + "'") # noqa
                else:
                    set_values.append("`" + k + "`" + " = " + str(v)) # noqa

        pk_len = len(record["primary_key_columns"])
        for i in range(0, pk_len):
            if isinstance(record["data"][record["primary_key_columns"][i]], str): # noqa
                where_values.append(record["primary_key_columns"][i] + " = '" + record["data"][record["primary_key_columns"][i]] + "'") # noqa
            else:
                where_values.append(record["primary_key_columns"][i] + "=" + str(record["data"][record["primary_key_columns"][i]])) # noqa
            if i < (pk_len - 1):
                where_values.append(" AND ")

        sql = "UPDATE {} SET {} WHERE {}".format(table_name, ", ".join(x for x in set_values), " ".join(x for x in where_values)) # noqa
        logger.debug("Generated SQL for update: {}".format(sql))
        return sql

    @staticmethod
    def gen_insert_col_list(record: dict) -> str:
        column_str = '`' + ' '.join(map(str, (k for k in record["data"]))).replace(' ', ",").replace(',', ', `').replace(',', '`,') + '`' # noqa
        logger.debug("column string: {}".format(column_str))
        return column_str

    @staticmethod
    def gen_insert_value_list(record: dict) -> str:
        values = list()
        for k, v in record["data"].items():
            if isinstance(v, int) or isinstance(v, float):
                values.append(str(v))
            elif isinstance(v, dict):
                values.append(escape_string(json.dumps(v)))
            elif v is None or v == "NULL":
                values.append("NULL")
            else:
                logger.debug("value: {}".format(v))
                values.append("'" + escape_string(v) + "'")
        return ", ".join(x for x in values)
