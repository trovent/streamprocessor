# TSP - Trovent Stream Processor #

Trovent Stream Processor is a Java application that implements a rest api to control an esper engine used for stream processing.
You can add and remove schemas and statements to the engine via rest api calls.
Additionally it can be directly connected to a running kafka instance to read data from a topic as a consumer and write into another topic as a producer.

##### Prerequisites to run it locally

Move to the directory which contains Maven parent project - `streamprocessor/tsp/app`

##### How to compile

	$ ./mvnw compile
	
##### How to run tests

	$ ./mvnw verify
	
##### How to run application

	$ ./mvnw -pl webapp spring-boot:run

When you want to use a configuration file of your own you can provide it with a command line switch:

    $ ./mvnw -pl webapp spring-boot:run -Dspring-boot.run.arguments=--kafka.configFileLocation=app.local.properties
	
##### Quick check if app is up and running (could take few seconds to run previous command)

	$ curl http://localhost:8080/api/status


### Creating a demonstration environment

To make TSP fully usable you have to run a kafka instance that TSP can connect to.
A docker container running Kafka can be started executing

	$ docker-compose up -d kafka
    
If you want to run dockerized version of tsp then execute:

    $ docker-compose up -d tsp

After that you have to add some definitions to it (schema, statement and connection to topics):

	$ ./restdemo/add_demo_definitions.sh
	
Run a console consumer to watch the output channel of kafka:

	$ ./run_consumer.sh output
	
Now feed kafka with input:

	$ ./restdemo/send.sh
	
Feel free to change the data that is sent and modify or add statements!

### Rest API Documentation

Can be accessed at `http://localhost:8080/api/swagger-ui.html` after the java application is up and running.

 