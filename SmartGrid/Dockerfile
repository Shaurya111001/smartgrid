FROM eclipse-temurin:21-jre
VOLUME /tmp
ARG SERVICE_NAME
COPY ${SERVICE_NAME}/target/${SERVICE_NAME}-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
