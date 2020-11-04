from AivenHW.Consumer import Consumer
from AivenHW.DBWriter import DBWriter
import getopt
import psycopg2
from psycopg2 import sql
import queue
import sys
import threading


def print_help():
    """
    Print help
    """
    print("consumer_app -p <properties_file> -d <postgresql_uri>")
    print("  -p: The consumer properties file.")
    print("  -d: The postgreSQL database URI.")

def parse_cmd_line_args(argv):
    """
    Parse command line arguments.
    :param argv: List of command line arguments to be parsed.
    :return: consumer properties file and the database URI.
    """

    file = ""
    uri = ""

    try:
        opts, args = getopt.getopt(argv, "p:d:", [])
    except getopt.GetoptError:
        print_help()
        sys.exit()

    for opt, arg in opts:
        if opt == '-p':
            file = arg
        elif opt == '-d':
            uri = arg
        else:
            print_help()

    return file, uri

def create_table(uri, table):
    """
    Create the table in database if already does not exist. In this example application we
    will create a simple table with a 'serial' type as primary key and a 'varchar' data.
    :param uri: URI of the database where the table is to be created.
    :param table: The name of the table to be created.
    :return:
    """
    try:
        db_conn = psycopg2.connect(uri)
        cursor = db_conn.cursor()

        cursor.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {} (id serial PRIMARY KEY, data varchar);")
                .format(sql.Identifier(table)))

        db_conn.commit()
        cursor.close()
        db_conn.close()
    except Exception as e:
        print(f"Caught exception while creating table in URI={uri}: {e}")
        raise e


# __name__ == __main__
msg_queue = queue.Queue()
queue_lock = threading.Lock()

properties_file, db_uri = parse_cmd_line_args(sys.argv[1:])

table_name = "aiven_hw"
create_table(db_uri, table_name)
insert_command = f"INSERT INTO {table_name} (data) VALUES (%s)"

db_writer = DBWriter(msg_queue, queue_lock, db_uri, insert_command)
db_writer.start()

consumer = Consumer(msg_queue, queue_lock, "test-topic", properties_file)
consumer.start()

while True:
    line = input("Enter exit() to quit\n")
    if line == "exit()":
        break

db_writer.stop()
consumer.stop()

try:
    db_writer.join()
    consumer.join()
except Exception as e:
    print(f"ERROR: Caught exception: {e}")