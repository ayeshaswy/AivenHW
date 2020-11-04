import getopt
from AivenHW.Producer import Producer
import queue
import sys
import threading
import time


def print_help():
    """
    Print help
    """
    print("producer_app -p <properties_file> -n <number_of_messages>")
    print("  -p: The producer properties file.")
    print("  -n: Number of messages the producer should send.")


def parse_cmd_line_args(argv):
    """
    Parse command line arguments.
    :param argv: List of command line arguments to be parsed.
    :return: Producer properties file and the number of messages to send.
    """

    file = ""
    msgs = 0

    try:
        opts, args = getopt.getopt(argv, "p:n:", [])
    except getopt.GetoptError:
        print_help()
        sys.exit()

    for opt, arg in opts:
        if opt == '-p':
            file = arg
        elif opt == '-n':
            try:
                msgs = int(arg)
            except ValueError as e:
                print(e, ": Invalid input for num-msgs.")
                print_help()
                sys.exit()
        else:
            print_help()

    return file, msgs


def enqueue_msg(message):
    queue_lock.acquire()
    msg_queue.put_nowait(message)
    queue_lock.release()
    print(f"Sending: {message}")


# __name == __main__
msg_queue = queue.Queue()
queue_lock = threading.Lock()

properties_file, num_msgs = parse_cmd_line_args(sys.argv[1:])

producer = Producer(msg_queue, queue_lock, "test-topic", properties_file)
producer.start()

for i in range(1, num_msgs + 1):
    enqueue_msg(f"Message number {i}")

msg_queue.join()

producer.stop()

try:
    producer.join()
except Exception as e:
    print(f"ERROR: Caught exception: {e}")