FROM openjdk:11
VOLUME /tmp
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar"]

ADD target/miningbee-docker-1.0-jar-with-dependencies.jar /app.jar
