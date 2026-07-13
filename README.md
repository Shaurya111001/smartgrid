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
- CMake 3.16+ and a C++17 compiler (for the optional MPI load simulator in `simulation/`)
- Open MPI and librdkafka, only if you want the simulator to actually publish to Kafka (see
  [Running the MPI Simulator](#running-the-mpi-simulator)) — without them it still builds and
  runs, just as a single-process, Kafka-less stub

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

## Running the MPI Simulator

`simulation/` is a standalone C++17/CMake project (independent of the Maven build) that models
grid districts of producer/consumer/accumulator nodes and, if built with Kafka support, publishes
directly to `measurement-events` — the same topic `measurement-service`'s REST endpoint feeds. Use
it to load-test the pipeline or exercise `analytics-service` without going through the REST API.

### 1. Install the optional native dependencies (macOS/Homebrew)
```bash
brew install open-mpi librdkafka
```
Without these, CMake automatically falls back to a single-process build with Kafka publishing
compiled out (`kafka/event_publisher.cpp`'s stub branch) — useful for a quick local test of the
simulation logic itself, but it won't produce anything on Kafka.

### 2. Build
```bash
cmake -S simulation -B simulation/build
cmake --build simulation/build
```
Check the CMake configure output for both of these lines — if either is missing, the simulator
will silently run without Kafka:
```
-- Found MPI_CXX: ...
-- Found librdkafka: ... (headers: .../include/librdkafka)
```

### 3. Run it against the live Kafka broker
Kafka must already be up (`docker compose up -d kafka` at minimum) and reachable at
`localhost:9092` — the simulator runs on the host, not in Docker, so it uses the
external/host-mapped listener, not the internal `kafka:29092` address the other services use.
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
mpirun -np 2 --oversubscribe ./simulation/build/simulation_app [scenario] [strategy] [district_count]
```
Args (all optional, shown with their defaults):
- `scenario`: `sparse` (default, few nodes/district) or `dense` (hundreds of nodes/district)
- `strategy`: `roundrobin` (default), `weighted`, or `nodepartition` — see below
- `district_count`: number of districts to generate (default `4`)

Three partitioning strategies are available, to satisfy the assignment's requirement to compare
different task-allocation mappings:
- **`roundrobin`** (`PartitionStrategy::district_partition`): whole districts assigned round-robin
  to ranks. Zero inter-rank communication, but imbalanced if district sizes vary.
- **`weighted`** (`PartitionStrategy::weighted_district_partition`): whole districts, greedily
  assigned to the currently least-loaded rank by node count. Still zero communication, but stays
  balanced even with mixed dense/sparse districts in the same run.
- **`nodepartition`** (`PartitionStrategy::node_partition`): individual *nodes* (not whole
  districts) are split across ranks. This is the only strategy with real inter-rank communication —
  every rank computes a partial sum for every district's locally-owned nodes, then all ranks
  combine their partial sums via a collective `MPI_Allreduce` (see
  `District::simulate_step_distributed`) to get the true district-wide balance. This is what gives
  you actual, non-zero "communication overhead" numbers to compare against the other two.

Each run's results are tagged by configuration in `results/<scenario>_<strategy>_p<N>/`, including
a `benchmark.csv` with per-rank timing — so you can run all three strategies (and both scenarios)
and diff the CSVs directly instead of copy-pasting terminal output. For example, to build the
comparison the assignment asks for:
```bash
for strategy in roundrobin weighted nodepartition; do
  mpirun -np 4 --oversubscribe ./simulation/build/simulation_app dense $strategy 8
done
cat results/dense_*_p4/benchmark.csv
```

### 4. Verify the full MPI → Kafka → Spark flow
```bash
# Confirm the simulator's own messages landed in measurement-events
docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic measurement-events --from-beginning --timeout-ms 15000 2>/dev/null | grep MEASUREMENT | head -5

# Confirm analytics-service picked them up and computed real (non-zero) per-district totals
docker exec smartgrid-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic district-soc-state --from-beginning --timeout-ms 15000 2>/dev/null | tail -5
```
You should see `current_SOC` values for districts `1` and `2` (the simulator's synthetic
districts), alongside `D-Central` (from any REST-submitted measurements) — proof the same
analytics job is correctly aggregating both sources. `current_SOC` reflects only accumulators'
own applied (capacity-clamped) charge deltas, so it should stay bounded and physically plausible
(roughly in the hundreds, not growing without limit) — if you see it climbing into the thousands
unboundedly, something regressed (see the "district-soc-state shows 0.0" troubleshooting entry
below for the related failure mode).

> **Note:** the simulator's district/node IDs (small integers like `"1"`, `"2"`) are synthetic and
> don't correspond to any node registered via Node Manager, so `billing-service` won't be able to
> resolve them to a real user — this simulator is for analytics/load-testing, not billing.

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
- **`simulation_app` builds but never publishes to Kafka, even with librdkafka installed:** older
  versions of `kafka/event_publisher.cpp` checked `#ifdef RDKAFKA_LIB` (a stray CMake *variable*
  name), while `CMakeLists.txt` only ever defines the compiler macro `RDKAFKA_AVAILABLE` — so the
  Kafka code path silently compiled out as dead code. This is fixed (both now use
  `RDKAFKA_AVAILABLE`); if you see this again after editing the source, check for the mismatch.
- **CMake configure warns `librdkafka not found` despite `brew install librdkafka`:** on Apple
  Silicon, `/opt/homebrew` isn't always on CMake's default search path, and Homebrew installs
  `rdkafka.h` under `include/librdkafka/` while the code includes it as `<rdkafka.h>` (no
  subdirectory) — `CMakeLists.txt`'s `find_library`/`find_path` calls now pass explicit `HINTS`
  and `PATH_SUFFIXES librdkafka` to handle both. If it still fails, confirm with
  `brew --prefix librdkafka` that the paths match what's hinted.
- **`simulation_app` crashes with `Abort trap: 6` / a libmalloc "pointer being freed was not
  allocated" error on exit:** `rd_kafka_new()` takes ownership of the `rd_kafka_conf_t*` passed to
  it and frees it internally on success; the old destructor also called `rd_kafka_conf_destroy()`
  on it — a double-free. Fixed by clearing the cached `conf` pointer after a successful
  `rd_kafka_new()` call.
- **`district-soc-state` shows `0.0` for a district that should have real producer/consumer
  activity:** `analytics-service`'s CASE WHEN only signs measurements where `type` is exactly
  `producer` or `consumer` (lowercase, per `kafka-topics.md`). Check whatever published the
  measurement is using those literal values, not something else (e.g. `SmartMeter`,
  `PRODUCTION`/`CONSUMPTION`) — both the REST example above and `simulation/domain/district.cpp`
  were fixed to match this shape.

## License

[Add license information here]