from contextlib import contextmanager
import os
import MySQLdb
from dotenv import load_dotenv
import unittest


class MySQLRecordsDB(unittest.TestCase):
    @contextmanager
    def managed_cursor(self):
        cursor = self.db.cursor()
        cursor.execute(f"USE {self.db_name}")
        try:
            yield cursor
        finally:
            cursor.close()

    def seed_db(self):
        with self.managed_cursor() as cursor:
            cursor = self.db.cursor()
            records = [
                # Seznam
                [
                    "seznam.cz",
                    "2021-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2021-05",
                    100,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-01 00:00:00.000",
                    "filename1",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-02 00:00:00.000",
                    "filename2",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2022-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2022-05",
                    200,
                    "warc",
                ],
                [
                    "seznam.cz",
                    "2023-01-03 00:00:00.000",
                    "filename3",
                    100,
                    200,
                    "CC-MAIN-2023-09",
                    200,
                    "warc",
                ],
            ]
            values = ", ".join(
                [
                    f"('{record[0]}', '{record[1]}', '{record[2]}', {record[3]}, {record[4]}, '{record[5]}', {record[6]}, '{record[7]}')"
                    for record in records
                ]
            )
            insert_data_sql = f"""
                INSERT INTO {self.table_name} (url, fetch_time, warc_filename, warc_record_offset, warc_record_length, crawl, fetch_status, subset)
                VALUES {values}
            """
            cursor.execute(insert_data_sql)
            self.db.commit()

    def remove_db(self):
        cursor = self.db.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        self.db.commit()
        cursor.close()

    def setUp(self):
        # Connect to MySQL
        load_dotenv()
        self.db = MySQLdb.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            passwd=os.getenv("MYSQL_PASSWORD"),
            port=int(os.getenv("MYSQL_PORT")),
        )
        self.db_name = os.getenv("MYSQL_DB_NAME")
        self.table_name = os.getenv("MYSQL_TABLE_NAME")

        # Create a Cursor object to execute SQL commands
        self.remove_db()
        cursor = self.db.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        cursor.close()
        self.db.commit()

        with self.managed_cursor() as cursor:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    url VARCHAR(255),
                    fetch_time TIMESTAMP,
                    warc_filename VARCHAR(255),
                    warc_record_offset INT,
                    warc_record_length INT,
                    crawl VARCHAR(50),
                    fetch_status INT,
                    subset VARCHAR(50)
                )
            """
            cursor.execute(create_table_sql)
            self.db.commit()

        self.seed_db()

    def tearDown(self):
        self.remove_db()
        self.db.close()
