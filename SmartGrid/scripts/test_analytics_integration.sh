#!/bin/bash
# Simple smoke test: POST a measurement and poll presentation analytics until a result appears
set -euo pipefail

MEASUREMENT_URL="http://localhost:8083/measurements"
PRESENTATION_URL="http://localhost:8085/analytics/districts"
NODE_ID="$1"   # pass node id as first arg
if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <nodeId>"
  exit 2
fi

# Send a measurement
curl -s -X POST "$MEASUREMENT_URL" -H "Content-Type: application/json" -d "{\"nodeId\": \"$NODE_ID\", \"energyValue\": 15.0}" > /dev/null

# Poll for analytics (timeout 30s)
end=$((SECONDS+30))
while [ $SECONDS -lt $end ]; do
  resp=$(curl -s "$PRESENTATION_URL")
  if [[ "$resp" != "[]" && "$resp" != "null" ]]; then
    echo "Analytics output found:"
    echo "$resp" | jq '.' || echo "$resp"
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for analytics output"
exit 1
