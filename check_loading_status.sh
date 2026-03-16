#!/bin/bash

echo "=== LinkedTCGA-M Status ==="
echo "Triple count:"
curl -s 'http://localhost:8887/sparql?query=SELECT+COUNT%28*%29+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D' -H 'Accept: application/sparql-results+json' | python3 -c "import sys, json; data = json.load(sys.stdin); print('  ', data['results']['bindings'][0]['callret-0']['value'], 'triples')"

echo ""
echo "CPU/Memory usage:"
docker stats --no-stream largerdfbench-linkedtcga-m-1 --format "  CPU: {{.CPUPerc}}  Memory: {{.MemUsage}}"

echo ""
echo "=== LinkedTCGA-E Status ==="
echo "Triple count:"
curl -s 'http://localhost:8888/sparql?query=SELECT+COUNT%28*%29+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D' -H 'Accept: application/sparql-results+json' | python3 -c "import sys, json; data = json.load(sys.stdin); print('  ', data['results']['bindings'][0]['callret-0']['value'], 'triples')"

echo ""
echo "CPU/Memory usage:"
docker stats --no-stream largerdfbench-linkedtcga-e-1 --format "  CPU: {{.CPUPerc}}  Memory: {{.MemUsage}}"

echo ""
echo "Run this script again to monitor progress!"
