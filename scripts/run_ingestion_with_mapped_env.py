#!/usr/bin/env python3
"""
Wrapper script to run ingestion commands with mapped environment variables.
Loads .env file and mapped variables, then runs the ingestion command.
"""
import os
import sys
import subprocess
from pathlib import Path

# Load .env file
from dotenv import load_dotenv
load_dotenv()

# Load mapped environment variables if they exist
mapped_env_file = "/tmp/github_env_ingest.txt"
if os.path.exists(mapped_env_file):
    with open(mapped_env_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key] = value

# Run the ingestion command
if len(sys.argv) < 2:
    print("Usage: python3 scripts/run_ingestion_with_mapped_env.py <ingestion_option>", file=sys.stderr)
    sys.exit(1)

ingestion_option = sys.argv[1]
result = subprocess.run(
    [sys.executable, "-m", "ingestion.main", ingestion_option],
    env=os.environ
)
sys.exit(result.returncode)

