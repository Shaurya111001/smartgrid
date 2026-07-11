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
