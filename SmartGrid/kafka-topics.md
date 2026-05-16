# Kafka topics used by SmartGrid

This document lists the canonical Kafka topics and JSON shapes used by the SmartGrid project.

Topics

- measurement-events
  - Source: Measurement Service (REST POST /measurements) or Simulation
  - Key: districtId (recommended)
  - Value (JSON):
    {
      "eventType": "MeasurementReported",
      "nodeId": "node-123",
      "districtId": "district_A",
      "type": "producer",        // or "consumer" or "accumulator"
      "energyValue": 12.5,        // positive number
      "timestamp": "2026-05-14T13:45:30Z" // ISO-8601 string
    }

- node-events
  - Source: Node Manager
  - Sample JSON: { "eventType":"NodeCreated", "nodeId":"n1", "districtId":"D1", "type":"producer" }

- district-window-stats
  - Produced by analytics-service (Spark)
  - Value: windowed aggregate JSON (includes districtId, window start/end, net_district_balance)

- district-soc-state
  - Produced by analytics-service (Spark)
  - Value: per-district current SOC JSON (includes districtId, current_SOC)

Quick test (after services + Kafka are up)

1. POST a measurement (replace NODE_ID and USER_ID with valid ones):

```bash
curl -X POST http://localhost:8083/measurements \
  -H "Content-Type: application/json" \
  -d '{"nodeId":"node-1","energyValue":12.5}'
```

2. Observe analytics topic (console consumer):

```bash
# List topics
docker exec smartgrid-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume windowed stats
docker exec -it smartgrid-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic district-window-stats --from-beginning
```

Notes

- Ensure producers set the record key to `districtId` for per-district ordering.
- The analytics job expects `timestamp` to be an ISO-8601 string; Jackson default (Instant) produces this format. If you change serialization, update `AnalyticsEngine` accordingly.
