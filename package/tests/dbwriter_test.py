import unittest
import psycopg2
from psycopg2 import sql
import queue
import threading
import time

from AivenHW.DBWriter import DBWriter
from AivenHW.Properties import load_properties

class DBWriterTest(unittest.TestCase):
    """Tests for Consumer class"""

    def setUp(self) -> None:
        self.properties_file = "dbwriter_test.properties"
        self.properties = load_properties(self.properties_file)

        self.msg_queue = queue.Queue()
        self.queue_lock = threading.Lock()

        self.db_conn = psycopg2.connect(self.properties["uri"])
        self.cursor = self.db_conn.cursor()

    def tearDown(self) -> None:
        self.cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(self.properties["table_name"])))

        self.db_conn.commit()
        self.cursor.close()
        self.db_conn.close()

    def create_table(self):
        """
        Creates a simple table with name as provided in properties, with a 'serial' type as
        primary key and a 'varchar' data.
        """
        self.cursor.execute(
            sql.SQL("CREATE TABLE {} (id serial PRIMARY KEY, data varchar)")
                .format(sql.Identifier(self.properties["table_name"])))

        self.db_conn.commit()

    def get_values_from_table(self):
        """
        Fetched entries from the table given in properties.
        :return:
        """
        self.cursor.execute(
            sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.properties["table_name"])))

        return self.cursor.fetchall()

    def test_dbwriter_success(self):
        """
        Verify that DBWriter thread is successfully created and that it reads the messages
        from the queue and writes to DB table successfully.
        """
        self.create_table()

        insert_command = "INSERT INTO {} (data) VALUES (%s)".format(self.properties["table_name"])

        try:
            test_dbwriter = DBWriter(self.msg_queue, self.queue_lock, self.properties["uri"], insert_command)
        except Exception as e:
            self.fail(f"test_dbwriter_success() failed with exception: {e}")

        try:
            test_dbwriter.start()
        except Exception as e:
            self.fail(f"test_dbwriter.start() in test_create_consumer() failed with exception: {e}")

        self.assertEqual(2, threading.active_count()) # Main thread, DBWriter thread.

        msgs = []
        for i in range(1, 4):
            msg = f"Message number {i}"

            self.queue_lock.acquire()
            self.msg_queue.put_nowait(msg)
            self.queue_lock.release()

            msgs.append((i, msg))

        # Sleep for 5 seconds to allow the DB to be updated.
        time.sleep(5)
        msgs_in_db = self.get_values_from_table()

        self.assertEqual(msgs_in_db, msgs)

        test_dbwriter.stop()
        test_dbwriter.join()
        self.assertEqual(1, threading.active_count())

    def test_dbwriter_fail(self):
        """
        Verify that DBWriter thread raises exception when there is no table to insert messages.
        """

        # Table not created.

        insert_command = "INSERT INTO {} (data) VALUES (%s)".format(self.properties["table_name"])

        try:
            test_dbwriter = DBWriter(self.msg_queue, self.queue_lock, self.properties["uri"], insert_command)
            test_dbwriter.start()
        except Exception as e:
            self.fail(f"test_dbwriter_success() failed with exception: {e}")

        for i in range(1, 4):
            msg = f"Message number {i}"

            self.queue_lock.acquire()
            self.msg_queue.put_nowait(msg)
            self.queue_lock.release()

        # Sleep for 5 seconds to allow the DB to be updated.
        time.sleep(5)

        failure = False
        try:
            test_dbwriter.stop()
            test_dbwriter.join()
        except Exception as e:
            failure = True

        self.assertTrue(failure)



# __name__ == __main__

#unittest.main(verbosity=2)