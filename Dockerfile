FROM docker.g42.ae/docker-dmi/docker-hub/maven:3.6.0-jdk-8 AS builder

# copy the project files
COPY pom.xml pom.xml

ENV TZ UTC

# build all dependencies
RUN mvn dependency:go-offline -B

# copy your other files
COPY src src

# build for release
RUN mvn package -DskipTests=true

# our final base image
FROM docker.g42.ae/docker-dmi/docker-hub/openjdk:8u171-jre-alpine

RUN addgroup -g 6666 -S appuser && adduser -u 6666 -S appuser -G appuser

USER appuser

# set deployment directory
WORKDIR tora-scheduler

# copy over the built artifact from the maven image
VOLUME /tmp
COPY --from=builder target/tora-scheduler-0.0.1-SNAPSHOT.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
