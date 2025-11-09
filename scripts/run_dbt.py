import os
import sys
from dotenv import load_dotenv
import subprocess

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

args = sys.argv[1:]

dbt_dir = os.path.join(os.path.dirname(__file__), "..", "dbt_pklyn")

subprocess.run(
    ["dbt"] + args,
    cwd=dbt_dir,
    check=True,
)
