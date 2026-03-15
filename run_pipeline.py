import subprocess
import sys


print("Ingestion")
result = subprocess.run([sys.executable, "ingest.py"])
if result.returncode != 0:
    print("ingest failed")
    sys.exit(1)

print("\nTransform")
result = subprocess.run([sys.executable, "transform.py"])
if result.returncode != 0:
    print("Transform failed")
    sys.exit(1)

print("\nSQL Load")
result = subprocess.run([sys.executable, "sql_load.py"])
if result.returncode != 0:
    print("SQL Load failed")
    sys.exit(1)

print("\n complete.")