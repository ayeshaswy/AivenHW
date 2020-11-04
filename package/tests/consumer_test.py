import unittest
import queue
import threading
import time

from AivenHW.Consumer import Consumer
from AivenHW.Producer import Producer

class TestConsumer(Consumer):
    """Test consumer class inheriting from Consumer class, adds some helper methods."""
    def __init__(self, msg_queue, queue_lock, topic, properties_file):
        super(TestConsumer, self).__init__(msg_queue, queue_lock, topic, properties_file)

        self.msg_queue = msg_queue
        self.queue_lock = queue_lock
        self.topic = topic
        self.properties_file = properties_file

    def dequeue_msgs(self):
        msgs = []
        self.queue_lock.acquire()
        while not self.msg_queue.empty():
            msgs.append(self.msg_queue.get_nowait())
        self.queue_lock.release()

        return msgs


class ConsumerTest(unittest.TestCase):
    """Tests for Consumer class"""

    def setUp(self) -> None:
        self.properties_file = "consumer_test.properties"
        self.producer_properties_file = "producer_test.properties"
        self.msg_queue = queue.Queue()
        self.queue_lock = threading.Lock()
        self.topic = "unittest_topic"


    def test_create_consumer(self):
        """
        Verify that consumer thread can be created and run successfully.
        """
        try:
            test_consumer = TestConsumer(self.msg_queue, self.queue_lock, self.topic, self.properties_file)
        except Exception as e:
            self.fail(f"test_create_consumer() failed with exception: {e}")

        try:
            test_consumer.start()
        except Exception as e:
            self.fail(f"test_consumer.start() in test_create_consumer() failed with exception: {e}")

        # Sleep for a couple seconds to allow the thread to come up.
        time.sleep(2)
        self.assertEqual(3, threading.active_count()) # Main thread, consumer thread, consumer-group hear-beat daemon.

        test_consumer.stop()
        test_consumer.join()
        self.assertEqual(2, threading.active_count())


    def test_consumer_read_messages(self):
        """
        Verify that consumer reads messages from Kafka broker and puts them in the queue
        it is supplied with. This also happens to verify that the producer reads from the
        supplied queue and sends the messages to Kafka broker successfully.
        """
        try:
            test_consumer = TestConsumer(self.msg_queue, self.queue_lock, self.topic, self.properties_file)
            test_consumer.start()
        except Exception as e:
            self.fail(f"test_consumer_read_messages() failed with exception: {e}")

        producer_msg_queue = queue.Queue()
        producer_queue_lock = threading.Lock()
        try:
            test_producer = Producer(producer_msg_queue, producer_queue_lock, self.topic, self.producer_properties_file)
            test_producer.start()
        except Exception as e:
            self.fail(f"test_consumer_read_messages() failed with exception: {e}")

        msgs = []

        for i in range(1, 4):
            msg = f"Message number {i}"

            producer_queue_lock.acquire()
            producer_msg_queue.put_nowait(msg)
            producer_queue_lock.release()

            msgs.append(msg)

        # Sleep for few seconds seconds to allow the consumer thread to process all the messages.
        time.sleep(20)

        self.assertEqual(test_consumer.dequeue_msgs(), msgs)

        test_producer.stop()
        test_consumer.stop()
        test_producer.join()
        test_consumer.join()



# __name__ == __main__

#unittest.main(verbosity=2)