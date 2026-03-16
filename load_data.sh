#!/bin/bash

# Script to bulk load data into Virtuoso containers

echo "Loading data for LinkedTCGA-M..."
docker exec largerdfbench-linkedtcga-m-1 /usr/local/virtuoso-opensource/bin/isql 1111 dba dba exec="ld_dir('/data/toLoad', '*.n3', 'http://linkedtcga-m'); rdf_loader_run();"

echo ""
echo "Checking LinkedTCGA-M loading status..."
docker exec largerdfbench-linkedtcga-m-1 /usr/local/virtuoso-opensource/bin/isql 1111 dba dba exec="SELECT * FROM DB.DBA.load_list WHERE ll_state = 0;"

echo ""
echo "Loading data for LinkedTCGA-E..."
docker exec largerdfbench-linkedtcga-e-1 /usr/local/virtuoso-opensource/bin/isql 1111 dba dba exec="ld_dir('/data/toLoad', '*.n3', 'http://linkedtcga-e'); rdf_loader_run();"

echo ""
echo "Checking LinkedTCGA-E loading status..."
docker exec largerdfbench-linkedtcga-e-1 /usr/local/virtuoso-opensource/bin/isql 1111 dba dba exec="SELECT * FROM DB.DBA.load_list WHERE ll_state = 0;"

echo ""
echo "Done! Loading process initiated."
echo "Note: The loading process may take several minutes to complete."
echo "You can check the triple count with:"
echo "  curl -s 'http://localhost:8887/sparql?query=SELECT+COUNT%28*%29+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D' -H 'Accept: application/sparql-results+json'"
echo "  curl -s 'http://localhost:8888/sparql?query=SELECT+COUNT%28*%29+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D' -H 'Accept: application/sparql-results+json'"
