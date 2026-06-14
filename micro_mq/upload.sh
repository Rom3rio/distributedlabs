#!/usr/bin/env bash

for i in {1..10}; do
  curl -X POST http://localhost:8080/ -H "Content-Type: application/json" -d "{\"user_Id\": \"user_1\", \"amount\": $i.0}"
  echo "Done!"
done
