import os

# Mapping based on your screenshot
datasets = {
    "LinkedTCGA-M": 8887, "LinkedTCGA-E": 8888, "LinkedTCGA-A": 8889,
    "ChEBI": 8890, "DBPedia-Subset": 8891, "DrugBank": 8892,
    "GeoNames": 8893, "Jamendo": 8894, "KEGG": 8895,
    "LMDB": 8896, "NYT": 8897, "SWDFood": 8898,
    "Affymetrix": 8899
}

compose_content = "version: '3'\nservices:\n"

for folder, port in datasets.items():
    # Use a docker-friendly name
    safe_name = folder.lower().replace(" ", "-")
    
    compose_content += f"""
  {safe_name}:
    image: tenforce/virtuoso:virtuoso7.2.5
    platform: linux/amd64
    ports:
      - "{port}:8890"
    environment:
      - DBA_PASSWORD=dba
      - SPARQL_UPDATE=true
      - DEFAULT_GRAPH=http://localhost:{port}/sparql
    volumes:
      - ./{folder}:/data/toLoad
    mem_limit: 2g
"""

with open("docker-compose.yml", "w") as f:
    f.write(compose_content)

print("✅ docker-compose.yml generated for 13 Virtuoso instances.")