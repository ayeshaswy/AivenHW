import psycopg2
from psycopg2 import sql
import re
import threading
import time


"""
References: https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
            https://stackoverflow.com/questions/13793399/passing-table-name-as-a-parameter-in-psycopg2
"""


class DBWriter(threading.Thread):
    """
    Class that inserts messages from the given queue, to the supplied database and table.
    It also creates the table if it does not exist.
    """
    def __init__(self, msg_queue, queue_lock, uri, insert_query):
        """
        DBWriter call init function.

        :param msg_queue: The message queue to get the messages from.
        :param queue_lock: The lock to acquire while using the queue.
        :param uri: The URI of the DB in which messages are to be written.
        :param insert_query: The format of the INSERT command to execute.
        """
        super(DBWriter, self).__init__()

        self.message_queue = msg_queue
        self.queue_lock = queue_lock

        self.uri = uri

        # Divide "INSERT" query in to usable parts.
        query = insert_query
        query = re.split("VALUES", insert_query, flags=re.IGNORECASE)
        self.query_command = query[0] + "VALUES"
        self.query_arg_format  = query[1]

        self.db_conn = None
        self.cursor = None

        self.stop_writing_thread = False

        self.exc = None

        # Maximum number of rows that allowed to be inserted with 1 insert query.
        # Number of seconds the writing thread will sleep for if no messages are found in queue.
        # Ideall such parameters must be configurable. Hard-coding them for ease of coding.
        self.max_rows_single_insert = 20
        self.writing_thread_sleep_sec = 0.2

    def run(self):
        """
        Overridden thread class run() method. Creates a new thread which reads messages from the queue and
        inserts them in to the given table in the database.
        :return:
        """

        try:
            self.db_conn = psycopg2.connect(self.uri)
            self.cursor = self.db_conn.cursor()

        except Exception as e:
            print(f"Caught exception while connecting to the DB URI={self.uri}.{e}")
            raise e

        self.__read_messages()

    def stop(self):
        """
        Sets the exit condition to stop the writing thread.
        :return:
        """
        self.stop_writing_thread = True

    def join(self):
        """
        Overridden join method, joins this thread and raises an exception to the calling thread if one was caught in this
        thread.
        :return:
        """
        super(DBWriter, self).join()
        if self.exc:
            raise self.exc

    def __cleanup(self):
        """
        Closes connection with DB and resets instance variables.
        :return:
        """
        self.cursor.close()
        self.db_conn.close()
        self.cursor = None
        self.db_conn = None
        self.stop_writing_thread = False

    def __dequeue_msgs(self):
        """
        Dequeues a message from the client provided queue.
        :return: msg - The message that is dequeued.
        """
        msgs = []
        self.queue_lock.acquire()
        while not self.message_queue.empty() and len(msgs) <= self.max_rows_single_insert :
            msgs.append(self.message_queue.get_nowait())
        self.queue_lock.release()

        return msgs

    def __read_messages(self):
        """
        Reads the messages from the client supplied queue and inserts them in to the
        client supplied table in the database.
        :return:
        """
        while self.stop_writing_thread is not True:
            msgs = self.__dequeue_msgs()
            if len(msgs) > 0:
                try:
                    args_str = ','.join(self.cursor.mogrify(self.query_arg_format, (x, )).decode("utf-8") for x in msgs)
                    self.cursor.execute(self.query_command + args_str)
                    self.db_conn.commit()
                except Exception as e:
                    #print(f"ERROR: Caught exception while inserting to table: {e}")
                    if self.exc is None:
                        self.exc = e
            else:
                time.sleep(self.writing_thread_sleep_sec)

        self.__cleanup()




