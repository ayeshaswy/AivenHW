import unittest
import queue
import threading
import time

from AivenHW.Producer import Producer

class ProducerTest(unittest.TestCase):
    """Tests for Consumer class"""

    def setUp(self) -> None:
        self.properties_file = "producer_test.properties"
        self.msg_queue = queue.Queue()
        self.queue_lock = threading.Lock()
        self.topic = "unittest_topic"


    def test_create_producer(self):
        """
        Verify that producer thread can be created and run successfully.
        """
        try:
            test_producer = Producer(self.msg_queue, self.queue_lock, self.topic, self.properties_file)
        except Exception as e:
            self.fail(f"test_create_producer() failed with exception: {e}")

        try:
            test_producer.start()
        except Exception as e:
            self.fail(f"test_producer.start() in test_create_consumer() failed with exception: {e}")

        # Sleep for a couple seconds to allow the thread to come up.
        time.sleep(2)
        self.assertEqual(3, threading.active_count()) # Main thread, producer thread, kafka-python sender daemon.

        test_producer.stop()
        test_producer.join()
        self.assertEqual(2, threading.active_count())


# __name__ == __main__

#unittest.main(verbosity=2)