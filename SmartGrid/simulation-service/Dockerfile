# Build context for this Dockerfile is the repo root (see docker-compose.yml),
# since it needs both ../simulation (C++/MPI source) and this service's jar.

# Stage 1: compile the MPI simulation binary for this container's OS/arch. The
# host's simulation/build/ binary may be built for a different OS/arch (e.g.
# macOS during local dev), so it can't just be copied in -- it must be rebuilt
# here, same as `cmake -S simulation -B simulation/build` would do on a Linux host.
# Must match the runtime stage's Ubuntu version below (eclipse-temurin:21-jre's
# base) -- OpenMPI's ABI isn't stable across major versions (5.x dropped the
# C++ bindings library entirely), so a mismatched build/runtime OS pairing
# links against a .so that the runtime's package repo won't provide.
FROM ubuntu:26.04 AS simulation-build
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake openmpi-bin libopenmpi-dev librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
COPY simulation /simulation-src
RUN cmake -S /simulation-src -B /simulation-build \
    && cmake --build /simulation-build -j "$(nproc)"

# Stage 2: runtime image. Like the other Spring services (see ../Dockerfile),
# the jar is expected to already be built on the host via `mvn package` --
# this Dockerfile doesn't run Maven, only packages the artifact.
FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends \
        openmpi-bin librdkafka1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=simulation-build /simulation-build/simulation_app /opt/simulation/simulation_app
COPY SmartGrid/simulation-service/target/simulation-service-0.0.1-SNAPSHOT.jar /app.jar
WORKDIR /opt/simulation
ENTRYPOINT ["java","-jar","/app.jar"]
