# SmartGrid

A microservices-based smart grid management system built with Spring Boot and Apache Kafka. This project demonstrates event-driven architecture for managing user accounts, smart grid nodes, energy measurements, billing, and data presentation.

## Architecture

The SmartGrid system consists of the following microservices:

- **Account Service** (Port 8081): Manages user registration and account information
- **Node Manager** (Port 8082): Handles smart grid node registration and management (e.g., smart meters)
- **Measurement Service** (Port 8083): Processes energy consumption measurements from nodes
- **Billing Service** (Port 8084): Calculates billing based on measurements and user accounts
- **Presentation Service** (Port 8085): Provides APIs for data presentation and frontend integration
- **Analytics Service**: A Spark Structured Streaming job (no REST port) that consumes `measurement-events` and produces `district-window-stats` (sliding-window net balance) and `district-soc-state` (cumulative state of charge) — see [Running Analytics Service](#running-analytics-service) below for the extra setup it needs

### Communication
Services communicate asynchronously using Apache Kafka for event-driven messaging. The system uses:
- **Kafka (KRaft mode)**: Self-managed message broker for inter-service communication — no Zookeeper dependency

### Frontend
A modern web portal (`frontend/index.html`) provides a user interface for managing energy nodes and viewing billing information.

## Prerequisites

- Java 21 (for the five Spring Boot services)
- Java 17 (for `analytics-service`, which targets Java 17 to match its Spark runtime)
- Maven 3.6+
- Docker and Docker Compose
- Internet access to Docker Hub (to pull `confluentinc/cp-kafka:7.6.0` and `apache/spark:3.5.3-java17-python3`)

## Setup and Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd smartgrid
   ```

2. **Build the services:**
   ```bash
   mvn clean package
   ```
   This also resolves `analytics-service`'s dependencies (including the Kafka connector,
   `spark-sql-kafka-0-10`) into your local `~/.m2/repository`, which `docker-compose.yml` mounts
   read-only into the analytics container — see [Running Analytics Service](#running-analytics-service).

3. **Drop old containers/images (if upgrading from Zookeeper):**
   If you previously ran the system with Zookeeper, you must tear down the old setup first:
   ```bash
   # Stop and remove all containers, volumes, and old images
   docker compose down --rmi all --volumes
   ```

4. **Start the infrastructure:**
   ```bash
   docker compose up -d --build
   ```

   This will start:
   - Kafka (KRaft mode — self-managed, no Zookeeper)
   - All microservices

5. **Verify services are running:**
   Check that all containers are up:
   ```bash
   docker compose ps
   ```

6. **Verify Kafka is running in KRaft mode:**
   ```bash
   # Check Kafka logs for KRaft initialization
   docker logs smartgrid-kafka 2>&1 | grep -i "kraft\|raft\|quorum"

   # Verify no Zookeeper dependency
   docker compose ps | grep -i zookeeper  # Should return nothing

   # List Kafka topics to confirm broker is healthy
   docker exec smartgrid-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

## Running the Application

Once the services are started, you can interact with the APIs:

### Register a User
```bash
curl -X POST http://localhost:8081/users/register \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "email": "alice@example.com"}'
```

### Add a Node
```bash
curl -X POST http://localhost:8082/nodes \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-id-here", "districtId": "D-Central", "type": "producer"}'
```
> `type` must be `producer`, `consumer`, or `accumulator` (see `kafka-topics.md`) — this is what
> `analytics-service` uses to sign each measurement (+/-) when computing district balances. Other
> values (e.g. `SmartMeter`) are accepted by Node Manager but are treated as neither producer nor
> consumer, so they don't contribute to the analytics output.

### Send a Measurement
```bash
curl -X POST http://localhost:8083/measurements \
  -H "Content-Type: application/json" \
  -d '{"nodeId": "node-id-here", "energyValue": 12.5}'
```

### View Users
```bash
curl http://localhost:8081/users
```

## Running Analytics Service

`analytics-service` is a Spark Structured Streaming job, not a Spring Boot service, so it needs
two things the other services don't:

1. **The Kafka connector on the classpath.** `spark-sql-kafka-0-10` isn't bundled in the Spark
   distribution, and this project doesn't ship a shaded/uber jar for it. Instead,
   `docker-compose.yml` mounts your local `~/.m2/repository` read-only into the container and
   passes the connector + its transitive deps via `spark-submit --jars`:
   ```yaml
   volumes:
     - ${HOME}/.m2/repository:/m2:ro
   command: [..., "--jars", "/m2/.../spark-sql-kafka-0-10_2.12-3.5.0.jar,..."]
   ```
   This only works if those jars are already cached locally — running `mvn clean package` (step 2
   above) takes care of that. If you ever bump the Spark or Kafka-connector version, re-run
   `mvn -pl analytics-service dependency:tree` to get the new resolved versions and update both
   the `--jars` list and the jar paths in `docker-compose.yml` to match.
2. **A Java 17 Spark image.** The image is pinned to `apache/spark:3.5.3-java17-python3` because
   `analytics-service` compiles to Java 17 bytecode, and the plain `apache/spark:3.5.x` tags ship
   a Java 11 JRE.

Bring it up and check it picked up the fix:
```bash
docker compose up -d analytics-service
docker logs -f analytics-service   # should show Kafka consumer/producer activity, no ClassNotFoundException
```

Verify it end-to-end by sending a measurement and reading the output topics:
```bash
curl -X POST http://localhost:8083/measurements -H "Content-Type: application/json" \
  -d '{"nodeId":"<NODE_ID>","energyValue":12.5}'

docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic district-soc-state --from-beginning --max-messages 1 --timeout-ms 15000
```
`district-window-stats` only emits once its 5-minute watermark passes, so don't expect immediate
output there — `district-soc-state` (complete-mode, no watermark) updates on every batch instead.

## Testing

Run the automated test script to verify end-to-end functionality:
```bash
./test_day5.sh
```

This script performs:
- User registration
- Node addition
- Measurement submission
- Fault recovery testing

### Fault Recovery, Manually
Every service rebuilds its entire state by replaying its Kafka topic(s) from the beginning on
startup (see `*ReplayService`/`*CacheService` classes), so killing and restarting any service
should never lose data:
```bash
curl -X POST http://localhost:8081/users/register -H "Content-Type: application/json" \
  -d '{"name":"Test User","email":"test@example.com"}'

docker stop account-service && docker start account-service
sleep 15   # give it time to reconnect to Kafka and replay user-events

curl http://localhost:8081/users   # the user registered above should still be there
```

## Development

### Building Individual Services
```bash
# Build all services
mvn clean package

# Build specific service
cd <service-name>
mvn clean package
```

### Running Services Locally (without Docker)
Each service can be run independently with proper Kafka configuration:

```bash
# Set KAFKA_BOOTSTRAP_SERVERS environment variable
export SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run a service
cd <service-name>
mvn spring-boot:run
```

### Adding New Services
1. Create a new module directory
2. Add it to the root `pom.xml` modules section
3. Update `docker-compose.yml` with the new service configuration

## Configuration

Each service uses `application.yml` for configuration. Key settings:
- Server port
- Kafka bootstrap servers
- Jackson serialization settings

## API Documentation

The services expose REST APIs. Refer to the test script (`test_day5.sh`) for example usage patterns.

## Troubleshooting

- **Services not starting:** Ensure Kafka is healthy before starting services (`docker compose ps` should show kafka as healthy)
- **Old Zookeeper containers lingering:** Run `docker compose down --rmi all --volumes` to clean up, then `docker compose up -d --build`
- **Connection issues:** Verify Docker network connectivity between containers
- **Build failures:** Ensure Java 21 (Java 17 for `analytics-service`) and Maven are properly installed
- **`analytics-service` fails to pull its image:** `bitnami/spark` tags are periodically retired
  from Docker Hub; this project now uses `apache/spark:3.5.3-java17-python3` instead. If that tag
  ever disappears too, check https://hub.docker.com/r/apache/spark/tags for a current
  `*-java17-*` tag and update `docker-compose.yml` (and the `--jars` versions if you also bump the
  Spark version — see [Running Analytics Service](#running-analytics-service)).
- **`analytics-service` logs `UnsupportedClassVersionError`:** the image is running an older JRE
  than `analytics-service` was compiled for. Confirm the image tag includes `-java17-` and that
  `maven.compiler.target` in `analytics-service/pom.xml` still matches.
- **`analytics-service` logs `ClassNotFoundException: Failed to find data source: kafka`:** the
  `--jars` paths in `docker-compose.yml` don't match what's in your `~/.m2/repository`. Run
  `mvn clean package` at the repo root first, then re-check the resolved versions with
  `mvn -pl analytics-service dependency:tree`.
- **`analytics-service` logs repeated `Connection to node -1 (localhost/127.0.0.1:9092) could not be established`:**
  it's using its default bootstrap-servers fallback instead of the configured one — verify
  `docker-compose.yml`'s `KAFKA_BOOTSTRAP_SERVERS` env var name matches what `AnalyticsEngine.java`
  reads via `System.getenv(...)`.
- **Restarting a service after a fresh restart shows fewer records than expected:** give it more
  than a couple of seconds — Kafka consumer-group rebalance on a brand-new `group.id` can take
  several seconds before any records are actually read back.

## License

[Add license information here]