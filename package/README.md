# AivenHW

AivenHW is the python package developed to address the requirement given in Aiven Homework for
the role of software engineer in support.


## Installation

Requirement:
1. Python 3.6 or newer.
2. pip


Download "AivenHW" package from "https://github.com/ayeshaswy/AivenHW/tree/main/package".

Execute: 
$$> pip install <path to AivenHW package>

The above command will also try to install the dependent packages:
1. jproperties 
2. Kafka-python
3. psycopg2


## Properties, keys and certificates

Make separate properties file for Producers and Consumers with the extention '.properties'.
Provide absolute paths of ca.pem, service.cert and service.key files in the properties file.
The format of the 'api_version' property is as as below:

api_version=2.6.0


## Usage


    from AivenHW.Producer import Producer
    Producer(msg_queue, queue_lock, topic, properties_file)

    where:

        msg_queue: 	The message queue for the producer to read messages from, to send to Kafka broker.
        queue_lock: The lock to acquire while using the queue.
        topic: 		The topic to send the messages to, at the kafka broker.
        properties_file: The properties file to use to configure the producer. Provide absolute path.
	
    Type: class
    Base class: threading.thread
    public methods: Base class public methods, stop()


    from AivenHW.Consumer import Consumer
    Consumer(msg_queue, queue_lock, topic, properties_file)

    where:

        msg_queue: 	The message queue to enqueue the incoming messages from Kafka broker.
        queue_lock: The lock to acquire while using the queue.
        topic: 		The topic to subscribe to at the kafka broker.
        properties_file: The properties file to use to configure the consumer. Provide absolute path.
	
    Type: class
    Base class: threading.thread
    public methods: Base class public methods, stop()


    from AivenHW.DBWriter import DBWriter
    DBWriter(msg_queue, queue_lock, uri, insert_query)

    where:

        msg_queue: 	The message queue to get the messages from.
        queue_lock: The lock to acquire while using the queue.
        uri: 		The URI of the DB in which messages are to be written.
        param insert_query: The format of the INSERT command to execute.
	
    Type: class
    Base class: threading.thread
    public methods: Base class public methods, stop()
	

    from AivenHW.Properties import load_properties
    load_properties(properties_file)

    where:
	
        properties_file: properties_file: The file to load the properties from. Provide absolute path.

    Type: method


## Unit tests

Unit tests can be found in the directory AivenHW/tests.
The tests are bare minimum and mostly test success scenrios.

Execute the tests by running the command:
$$> python -m unittest consumer_test.py producer_test.py dbwriter_test.py load_properties_test.py

