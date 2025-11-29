"""Fix alembic revision ID in database.

This script updates the alembic_version table to replace
'add_google_photo_url' with 'add_google_photo_name'.
"""

import os
import sys
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

pg_dsn = os.getenv("PG_DSN")
pg_schema = os.getenv("PG_SCHEMA", "public")

if not pg_dsn:
    print("ERROR: PG_DSN not set in environment")
    sys.exit(1)

try:
    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()
    
    # Check current version
    cur.execute(f'SELECT version_num FROM "{pg_schema}".alembic_version')
    current_version = cur.fetchone()
    
    if current_version:
        current = current_version[0]
        print(f"Current version in database: {current}")
        
        if current == "add_google_photo_url":
            # Update to the correct revision ID
            cur.execute(
                f'UPDATE "{pg_schema}".alembic_version SET version_num = %s WHERE version_num = %s',
                ("add_google_photo_name", "add_google_photo_url")
            )
            conn.commit()
            print("✓ Updated version from 'add_google_photo_url' to 'add_google_photo_name'")
        elif current == "add_google_photo_name":
            print("✓ Version is already correct: 'add_google_photo_name'")
        else:
            print(f"Current version is '{current}', no update needed")
    else:
        print("No version found in database")
    
    cur.close()
    conn.close()
    
except psycopg2.Error as e:
    print(f"Database error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

