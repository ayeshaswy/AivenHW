from kafka import KafkaConsumer
from AivenHW.Properties import load_properties
import threading

"""
References: https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
            https://docs.python.org/2/library/threading.html
            https://docs.python.org/3/library/queue.html
            https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
"""


class Consumer(threading.Thread):
    """
    Consumer class that reads messages from Kafka broker and enqueues the messages in the given queue.
    Inherits 'threading.thread'.
    """
    def __init__(self, msg_queue, queue_lock, topic, properties_file):
        """
        Consumer call init function.

        :param msg_queue: The message queue to enqueue the incoming messages from Kafka broker.
        :param queue_lock: The lock to acquire while using the queue.
        :param topic: The topic to subscribe to at the kafka broker.
        :param properties_file: The properties file to use to configure the consumer.
        """
        super(Consumer, self).__init__()

        # Load consumer properties.
        self.properties = load_properties(properties_file)

        self.consumer = KafkaConsumer(topic, **self.properties)

        self.message_queue = msg_queue
        self.queue_lock = queue_lock

        self.exc = None

        # Exit condition for the reading thread. After start() is called, the thread runs as long as the
        # flag is false, until stop() is called.
        self.stop_reading_thread = False

    def run(self):
        """
        Overridden thread class run() method. Creates a new thread which reads the messages from Kafka broker.
        :return:
        """
        self.__read_messages()

    def stop(self):
        """
        Sets the exit condition to stop the reading thread.
        :return:
        """

        # Set the exit flag to true to stop the the reading thread.
        self.stop_reading_thread = True

    def join(self):
        """
        Overridden join method, joins this thread and raises an exception to the calling thread if one was caught in this
        thread.
        :return:
        """
        super(Consumer, self).join()
        if self.exc:
            raise self.exc

    def __enqueue_msg(self, message):
        """
        Enqueues the supplied message to the given queue.
        :param message: The message to enqueue.
        :return:
        """
        self.queue_lock.acquire()
        self.message_queue.put_nowait(message)
        self.queue_lock.release()

    def __read_messages(self):
        """
        Read messages from Kafka broker.
        :return:
        """
        while self.stop_reading_thread is not True:
            raw_msgs = self.consumer.poll(timeout_ms=100)
            for topic, msgs in raw_msgs.items():
                for msg in msgs:
                    self.__enqueue_msg(msg.value.decode("utf-8"))

            try:
                self.consumer.commit()
            except Exception as e:
                #print(f"ERROR: Caught exception while commit() to kafka broker: {e}")
                if self.exc is None:
                    self.exc = e

        self.stop_reading_thread = False




