# testApp

Follow the instructions to run and test the test producer and consumer applications:

1. Install the package as described in README.md file in the path 'AivenHW/package/README.md'.
2. Create a properties file with '.properties' extention containing consumer properties. Check example file
   consumer.properties. The example file itself can be used which will use the kafka service running with my 
   account.
3. Create a properties file with '.properties' extention containing producer properties. Check example file
   producer.properties. The example file itself can be used which will use the kafka service running with my 
   account.
4. Ensure to create a topic by the name 'test-topic' in the supplied bootstrap-servers/kafka brokers.
   A topic with that name is already created in my kafka service if using my Kafka service. 
5. Provide the file paths for the properties 'ssl_cafile',  'ssl_certfile' and 'ssl_keyfile'ion the consumer
   and producer properties. If using the example files, make sure to use the given 'ca.pem', 'service.key' and
   'service.cert' files.
6. Start the consumer application with the following command:

   python consumer_app.py -p consumer.properties -d <database_uri>.
   
   The messages will be written to a table called 'aiven_hw' under the public schema.
   The table will contain only 2 fields; a serial number and a varchar data. 
7. Start the producer application with the following command:

   python producer_app.py -p producer.properties -n <number_of_messages_you_wish_to_send>
  
   The given number of messages will be sent to the Kafka broker which will be read by the consumer and written
   to the DB.
8. The producer application will end as soon as all the messages have been sent to the kafka broker.
9. The consumer application will run until an 'exit()' command is entered in the console terminal. The consumer
   application could also be run once for entire duration of testing whereas the producer could be run over
   multiple iterations.
 
 
# unit testing
1. Unit tests use topic name unittest_topic. If using my Kafka service, then a topic by that name is already created.
2. Inputs for all the test modules can be given in their respectice '.property' files.
3. The unit test covers bare minimum mostly covers only positive cases.
