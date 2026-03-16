#!/bin/bash

# Script to fix DirsAllowed in Virtuoso containers and load data

echo "Fixing LinkedTCGA-M container..."
docker exec largerdfbench-linkedtcga-m-1 sh -c "sed -i 's|DirsAllowed.*|DirsAllowed = ., /usr/share/proj, /data/toLoad|g' /virtuoso.ini"
docker restart largerdfbench-linkedtcga-m-1

echo "Waiting for LinkedTCGA-M to restart (30 seconds)..."
sleep 30

echo "Fixing LinkedTCGA-E container..."
docker exec largerdfbench-linkedtcga-e-1 sh -c "sed -i 's|DirsAllowed.*|DirsAllowed = ., /usr/share/proj, /data/toLoad|g' /virtuoso.ini"
docker restart largerdfbench-linkedtcga-e-1

echo "Waiting for LinkedTCGA-E to restart (30 seconds)..."
sleep 30

echo ""
echo "✓ Configuration fixed! Now you can load data via Virtuoso Conductor:"
echo ""
echo "LinkedTCGA-M: http://localhost:8887/conductor"
echo "LinkedTCGA-E: http://localhost:8888/conductor"
echo ""
echo "Login with username: dba, password: dba"
echo ""
echo "Then in Interactive SQL, run:"
echo "  ld_dir('/data/toLoad', '*.n3', 'http://linkedtcga-m');"
echo "  rdf_loader_run();"
echo ""
echo "(Replace graph URI with 'http://linkedtcga-e' for LinkedTCGA-E)"
