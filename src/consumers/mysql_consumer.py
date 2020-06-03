import json
import logging
from pymysql import connect, IntegrityError, ProgrammingError, escape_string
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from src.utils import get_secret
from src import constants

logger = logging.getLogger()
logger.setLevel("DEBUG")


class MySQLConsumer:
    def __init__(self, db_name):
        secret_string = get_secret("fmgmt-c1-maxwell")
        self.database_name = db_name
        self.__connection: Connection = connect(
            host=secret_string["host"],
            user=secret_string["username"],
            passwd=secret_string["password"],
            port=secret_string["port"],
            autocommit=True,
            db=db_name
        )
        self.__cursor: Cursor = self.__connection.cursor(Cursor)

    def process_row(self, data: dict) -> None:
        logger.debug("process_row, database = {}".format(data["database"]))
        sql = ""
        if data["type"] == constants.MAXWELL_INSERT_OP or data["type"] == constants.MAXWELL_BOOTSTRAP_OP: # noqa
            sql = self.__gen_insert_sql(data)
        elif data["type"] == constants.MAXWELL_UPDATE_OP:
            sql = self.__gen_update_sql(data)
        elif data["type"] == constants.MAXWELL_DELETE_OP:
            sql = self.__gen_delete_sql
        elif data["type"] == constants.MAXWELL_TABLE_CREATE_OP or data["type"] == constants.MAXWELL_TABLE_ALTER_OP: # noqa
            sql = data["sql"]
            logger.debug("SQL for DDL operation: {}".format(sql))
        else:
            logger.error("Unsupported DDL/DML operation: {}".format(data["type"])) # noqa
            logger.error("data dict for unsupported operation:  {}".format(json.dumps(data))) # noqa
            return

        if len(sql) > 0:
            self.__execute_statement(sql)
        else:
            logger.fatal("How did we get here, SQL stmt length is ZERO!")

    def close(self):
        try:
            self.__cursor.close()
            self.__connection.close()
        except Exception as e:
            logger.error("Error closing")
            logger.error(e)

    def __execute_statement(self, sql):
        logger.debug("Commmiting SQL")
        try:
            self.__cursor.execute(sql)
        except (IntegrityError, ProgrammingError) as error:
            logger.error("Integrity/Programming error SQL: {}".format(sql))
            logger.error(error)
            # send to DLQ ?
        except Exception as e:
            logger.error("Other error SQL: {}".format(sql))
            logger.error(e)

    def __gen_insert_sql(self, record: dict) -> str:
        table_name = record["table"]
        sql = "INSERT IGNORE INTO {} ( {} ) VALUES ( {} )".format(table_name,
                                                self.gen_insert_col_list(record), # noqa
                                                self.gen_insert_value_list(record)) # noqa
        logger.debug("Generated SQL for insert: {}".format(sql))
        return sql

    def __gen_update_sql(self, record: dict) -> str:
        table_name = record["table"]
        set_values = list()

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

        sql = "UPDATE {} SET {} WHERE {}".format(table_name,
                                                 ", ".join(x for x in set_values), # noqa
                                                 " ".join(x for x in self.gen_where_pk_clause(record))) # noqa
        logger.debug("Generated SQL for update: {}".format(sql))
        return sql

    def __gen_delete_sql(self, record: dict) -> str:
        table_name = record["table"]
        sql = "DELETE FROM {} WHERE {}".format(table_name, " ".join(x for x in self.gen_where_pk_clause(record))) # noqa
        logger.debug("Generated SQL for delete: {}".format(sql))
        return sql

    @staticmethod
    def gen_insert_col_list(record: dict) -> str:
        # yeah, this one hurt
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

    @staticmethod
    def gen_where_pk_clause(record: dict) -> list:
        where_values = list()
        pk_len = len(record["primary_key_columns"])
        for i in range(0, pk_len):
            if isinstance(record["data"][record["primary_key_columns"][i]], str): # noqa
                where_values.append(record["primary_key_columns"][i] + " = '" + record["data"][record["primary_key_columns"][i]] + "'") # noqa
            else:
                where_values.append(record["primary_key_columns"][i] + "=" + str(record["data"][record["primary_key_columns"][i]])) # noqa
            if i < (pk_len - 1):
                where_values.append(" AND ")
        return where_values
