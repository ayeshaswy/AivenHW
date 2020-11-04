from kafka import errors
from kafka import KafkaProducer
from AivenHW.Properties import load_properties
import threading
import time


"""
References: https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
            https://docs.python.org/2/library/threading.html
            https://docs.python.org/3/library/queue.html
            https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
"""

class Producer(threading.Thread):
    """
    Producer class that sends the messages enqueued in the given queue by the client, to Kafka broker.
    Inherits 'threading.thread'.
    """
    def __init__(self, msg_queue, queue_lock, topic, properties_file):
        """
        Producer call init function.

        :param msg_queue: The message queue for the producer to read messages from, to senf to Kafka broker.
        :param queue_lock: The lock to acquire while using the queue.
        :param topic: The topic to send the message to at the kafka broker.
        :param properties_file: The properties file to use to configure the producer.
        """
        super(Producer, self).__init__()

        self.properties = load_properties(properties_file)

        self.producer = KafkaProducer(**self.properties)

        self.message_queue = msg_queue
        self.queue_lock = queue_lock
        self.topic = topic

        self.exc = None

        self.stop_processing_thread = False
        self.processing_thread_sleep_sec = 0.2

    def run(self):
        """
        Overridden thread class run() method. Creates a new thread which sends messages to Kafka broker.
        :return:
        """
        self.__process_queue()

    def stop(self):
        """
        Sets the exit condition to stop the message sending thread.
        :return:
        """
        self.stop_processing_thread = True

    def join(self):
        """
        Overridden join method, joins this thread and raises an exception to the calling thread if one was caught in this
        thread.
        :return:
        """
        super(Producer, self).join()
        if self.exc:
            raise self.exc

    def __process_queue(self):
        """
        Gets messages from the given queue and sends them to Kafka broker.
        :return:
        """
        while self.stop_processing_thread is not True:
            self.queue_lock.acquire()
            if not self.message_queue.empty():
                message = self.message_queue.get_nowait()
                self.message_queue.task_done()
                self.queue_lock.release()
                try:
                    self.producer.send(self.topic, message.encode("utf-8"))
                    self.producer.flush()
                    #print(f"Sent: {message}")
                except errors.KafkaTimeoutError as kte:
                    #print(f"ERROR: Caught KafkaTimeoutError while sending {message} to broker: {kte}")
                    if self.exc is None:
                        self.exc = e
            else:
                self.queue_lock.release()
                time.sleep(self.processing_thread_sleep_sec)

        self.stop_processing_thread = False




