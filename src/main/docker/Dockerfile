FROM: openjdk:11-jdk-alpine
VOLUME /tmp
ADD miningbee-docker-1.0-jar-with-dependencies.jar app.jar
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar"]
