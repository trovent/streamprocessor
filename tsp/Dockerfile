FROM maven:3.6-jdk-8 AS build  
COPY app /usr/src/app
WORKDIR /usr/src/app
RUN mvn clean compile assembly:single

FROM openjdk:8-jdk-alpine
COPY --from=build /usr/src/app/target/tsp.jar /usr/app/tsp.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar", "/usr/app/tsp.jar"] 