#!/usr/bin/env python3
"""
Defines a logger with custom log formatter
"""
import re
import logging
from typing import List
import mysql.connector
from os import getenv


PII_FIELDS = ('name', 'email', 'phone', 'ssn', 'password')


def filter_datum(fields: List[str], redaction: str, message: str,
                 separator: str) -> str:
    """
    Filters message by replacing each value in fields with redaction
    """
    for field in fields:
        message = re.sub(field + "=.*?" + separator,
                         field + "=" + redaction + separator,
                         message)
    return message


class RedactingFormatter(logging.Formatter):
    """ Redacting Formatter class
    """
    REDACTION = "***"
    FORMAT = "[HOLBERTON] %(name)s %(levelname)s %(asctime)-15s: %(message)s"
    SEPARATOR = ";"

    def __init__(self, fields: List[str]):
        """Initializes Class"""
        super(RedactingFormatter, self).__init__(self.FORMAT)
        self.fields = fields

    def format(self, record: logging.LogRecord) -> str:
        """Formats the LogRecord instance"""
        log = super(RedactingFormatter, self).format(record)
        return filter_datum(self.fields, self.REDACTION, log, self.SEPARATOR)


def get_logger() -> logging.Logger:
    """Creates and configures a logger"""
    logger = logging.getLogger('user_data')
    logger.setLevel(logging.INFO)
    logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(RedactingFormatter(fields=PII_FIELDS))

    logger.addHandler(stream_handler)
    return logger


def get_db() -> mysql.connector.connection.MySQLConnection:
    """Connects to a mysql database"""
    connector = mysql.connector.connection.MySQLConnection(
        user=getenv('PERSONAL_DATA_DB_USERNAME', 'root'),
        password=getenv('PERSONAL_DATA_DB_PASSWORD', ''),
        host=getenv('PERSONAL_DATA_DB_HOST', 'localhost'),
        database=getenv('PERSONAL_DATA_DB_NAME'))

    return connector


def main() -> None:
    """Log database users"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users")
    fields = [i[0] for i in cursor.description]
    logger = get_logger()

    for row in cursor:
        str_row = ''.join(f'{f}={str(r)}; ' for r, f in zip(row, fields))
        logger.info(str_row.strip())
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()
