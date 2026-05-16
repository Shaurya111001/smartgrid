#!/bin/bash
export PATH=$PATH:/usr/local/bin:/Applications/Docker.app/Contents/Resources/bin/
set -e

echo "===== TESTING AUTOMATION ====="

# Wait for services to be ready
echo "Waiting 30 seconds for Spring Boot services to fully start..."
sleep 30

echo -e "\n--- PART 1: End-to-End Test ---"
echo "Registering user..."
curl -s -X POST http://localhost:8081/users/register \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "email": "alice@example.com"}' > /tmp/user_resp.json
cat /tmp/user_resp.json
echo ""
USER_ID=$(cat /tmp/user_resp.json | grep -o '"userId":"[^"]*' | cut -d'"' -f4)
echo "Extracted User ID: $USER_ID"
echo "Waiting 2 seconds for Kafka event to propagate..."
sleep 2

echo -e "\nAdding node..."
curl -s -X POST http://localhost:8082/nodes \
  -H "Content-Type: application/json" \
  -d "{\"userId\": \"$USER_ID\", \"districtId\": \"D-Central\", \"type\": \"SmartMeter\"}" > /tmp/node_resp.json
cat /tmp/node_resp.json
echo ""
NODE_ID=$(cat /tmp/node_resp.json | grep -o '"nodeId":"[^"]*' | cut -d'"' -f4)
echo "Extracted Node ID: $NODE_ID"
echo "Waiting 2 seconds for Kafka event to propagate..."
sleep 2

echo -e "\nSending measurement..."
curl -s -X POST http://localhost:8083/measurements \
  -H "Content-Type: application/json" \
  -d "{\"nodeId\": \"$NODE_ID\", \"energyValue\": 12.5}"
echo ""

echo -e "\n--- PART 2: Fault Recovery Test ---"
echo "Registering 3 more users..."
curl -s -X POST http://localhost:8081/users/register -H "Content-Type: application/json" -d '{"name": "User One", "email": "one@example.com"}' > /dev/null
curl -s -X POST http://localhost:8081/users/register -H "Content-Type: application/json" -d '{"name": "User Two", "email": "two@example.com"}' > /dev/null
curl -s -X POST http://localhost:8081/users/register -H "Content-Type: application/json" -d '{"name": "User Three", "email": "three@example.com"}' > /dev/null
echo "Done registering."

echo -e "\nCurrent users in Account Service:"
curl -s http://localhost:8081/users
echo ""

echo -e "\nStopping account-service..."
docker stop account-service

echo "Starting account-service..."
docker start account-service

echo "Waiting 15 seconds for account-service to recover state from Kafka..."
sleep 15

echo -e "\nVerifying users after recovery (Should show all 4 users!):"
curl -s http://localhost:8081/users
echo ""

echo -e "\nTesting Presentation Service (Nodes for User):"
curl -s http://localhost:8085/users/$USER_ID/nodes
echo ""

echo "===== TEST COMPLETE ====="

//
The `test_day5.sh` script is an automated integration test that verifies two major concepts of your architecture: **Event-Driven Communication** and **Event Sourcing (Fault Recovery)**. 

Here is a step-by-step breakdown of exactly how it tests your services:

### Part 1: End-to-End Data Flow
This part verifies that your services are successfully communicating with each other in real-time through Kafka events.

1. **Register a User (Account Service)**: It sends an HTTP POST request to the `account-service` to create "Alice Smith". It captures the unique `userId` returned by the server. The `account-service` publishes a `UserRegistered` event to Kafka.
2. **Add a Node (Node Manager)**: It waits 2 seconds (to let Kafka propagate the event) and then sends a POST request to `node-manager` to create a SmartMeter for Alice's `userId`. 
   - *What's being tested here:* If `node-manager` successfully created the node, it means our new `@KafkaListener` works perfectly! The `node-manager` successfully received the `UserRegistered` event from Kafka and knew Alice was a valid user.
3. **Send a Measurement (Measurement Service)**: It takes the generated `nodeId` and posts a reading of `12.5` energy units to the `measurement-service`.
   - *What's being tested here:* It verifies the `measurement-service` correctly received the `NodeCreated` event from Kafka and recognizes the `nodeId`.

### Part 2: Fault Recovery (Event Sourcing)
This part is the ultimate test of the "Event Sourcing" pattern. It proves that Kafka is the absolute source of truth and databases are completely rebuildable.

1. **Populate Data**: It registers 3 more random users so that there are 4 users total in the system.
2. **Simulate a Crash**: It runs `docker stop account-service` to forcefully kill the Account Service container. At this point, the Account Service loses all of its in-memory state.
3. **Restart the Service**: It runs `docker start account-service` to bring the container back online.
4. **Replay History**: It waits 15 seconds. During this time, the `account-service` connects to Kafka, starts from the very beginning of the `user-events` topic, and rebuilds its local database by replaying the history of the 4 user registrations.
5. **Verify Recovery**: It queries the `account-service` for all users. If it returns all 4 users, it proves the fault-recovery mechanism works perfectly!
6. **Verify Presentation View**: Finally, it asks the `presentation-service` for Alice's nodes. Since the `presentation-service` has been listening to both User and Node events this whole time, returning Alice's SmartMeter proves that the CQRS materialized views are updating correctly.
//
