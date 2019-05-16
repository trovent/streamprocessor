# TSP - Trovent Stream Processor #

Trovent Stream Processor is a Java application that implements a rest api to control an esper engine used for stream processing.
You can add and remove schemas and statements to the engine via rest api calls.
Additionally it can be directly connected to a running kafka instance to read data from a topic as a consumer and write into another topic as a producer.

##### How to compile

	$ mvn compile
	
##### How to run tests

	$ mvn verify
	
##### How to run application

	$ mvn exec:java
	
	
### Creating a demonstration environment

To make TSP fully usable you have to run a kafka instance that TSP can connect to.
A docker container running kafka can be started executing

	$ docker-compose up -d kafka
	
Now start Trovent Stream Processer:

	$ mvn exec:java

After that you have to add some definitions to it (schema, statement and connection to topics):

	$ ./demorest/add_demo_definitions.sh
	
Run a console consumer to watch the output channel of kafka:

	$ ./run_consumer.sh output
	
Now feed kafka with input:

	$ ./demorest/send.sh
	
Feel free to change the data that is sent and modify or add statements!

 