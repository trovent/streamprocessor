FROM java:openjdk-8-jdk as build
COPY app /usr/src/app
WORKDIR /usr/src/app
RUN ./mvnw clean install

FROM openjdk:8-jdk-alpine
COPY --from=build /usr/src/app/webapp/target/tsp.war /usr/app/tsp.war
EXPOSE 8080
ENTRYPOINT ["java","-jar", "/usr/app/tsp.war", "--kafka.configFileLocation=app.properties"] 