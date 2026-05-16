# SmartGrid

A microservices-based smart grid management system built with Spring Boot and Apache Kafka. This project demonstrates event-driven architecture for managing user accounts, smart grid nodes, energy measurements, billing, and data presentation.

## Architecture

The SmartGrid system consists of the following microservices:

- **Account Service** (Port 8081): Manages user registration and account information
- **Node Manager** (Port 8082): Handles smart grid node registration and management (e.g., smart meters)
- **Measurement Service** (Port 8083): Processes energy consumption measurements from nodes
- **Billing Service** (Port 8084): Calculates billing based on measurements and user accounts
- **Presentation Service** (Port 8085): Provides APIs for data presentation and frontend integration

### Communication
Services communicate asynchronously using Apache Kafka for event-driven messaging. The system uses:
- **Kafka (KRaft mode)**: Self-managed message broker for inter-service communication — no Zookeeper dependency

### Frontend
A modern web portal (`frontend/index.html`) provides a user interface for managing energy nodes and viewing billing information.

## Prerequisites

- Java 21
- Maven 3.6+
- Docker and Docker Compose

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
  -d '{"userId": "user-id-here", "districtId": "D-Central", "type": "SmartMeter"}'
```

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
- **Build failures:** Ensure Java 21 and Maven are properly installed

## License

[Add license information here]