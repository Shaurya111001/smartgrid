
# Docker
FROM ubuntu:26.04 AS simulation-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake openmpi-bin libopenmpi-dev librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
COPY simulation /simulation-src
RUN cmake -S /simulation-src -B /simulation-build \
    && cmake --build /simulation-build -j "$(nproc)"


FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends \
        openmpi-bin librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=simulation-build /simulation-build/simulation_app /opt/simulation/simulation_app
COPY SmartGrid/simulation-service/target/simulation-service-0.0.1-SNAPSHOT.jar /app.jar
WORKDIR /opt/simulation
ENTRYPOINT ["java","-jar","/app.jar"]
