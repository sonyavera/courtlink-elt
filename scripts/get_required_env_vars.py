#!/usr/bin/env python3
"""
Script to query the database and determine required environment variables
for GitHub Actions based on customer organizations.
"""
import os
import sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Get database connection info
pg_dsn = os.getenv("PG_DSN")
court_scraper_schema = os.getenv("COURT_SCRAPER_SCHEMA", "court_availability_scraper")  # Default schema name

if not pg_dsn:
    print("Error: PG_DSN environment variable not set", file=sys.stderr)
    sys.exit(1)

try:
    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()
    
    # Query organizations where is_customer = true
    query = f"""
    SELECT source_system_code, client_code, id, login_link, city, is_customer
    FROM {court_scraper_schema}.organizations
    WHERE is_customer = true
    ORDER BY source_system_code, client_code
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    # Get column names
    colnames = [desc[0] for desc in cur.description]
    
    print("=" * 80)
    print("Customer Organizations Found:")
    print("=" * 80)
    
    courtreserve_clients = []
    podplay_clients = []
    required_env_vars = set()
    
    for row in rows:
        org = dict(zip(colnames, row))
        source_system = org['source_system_code']
        client_code = org['client_code'].upper()
        
        print(f"\n{source_system.upper()} - {org['client_code']}:")
        print(f"  ID: {org['id']}")
        print(f"  Login: {org['login_link']}")
        print(f"  City: {org['city']}")
        
        if source_system == 'courtreserve':
            courtreserve_clients.append(org['client_code'])
            # CourtReserve needs: {client_code}_USERNAME and {client_code}_PASSWORD
            required_env_vars.add(f"{client_code}_USERNAME")
            required_env_vars.add(f"{client_code}_PASSWORD")
        elif source_system == 'podplay':
            podplay_clients.append(org['client_code'])
            # Podplay needs: {client_code}_API_KEY
            required_env_vars.add(f"{client_code}_API_KEY")
    
    cur.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("Summary:")
    print("=" * 80)
    print(f"\nCourtReserve clients: {', '.join(courtreserve_clients) if courtreserve_clients else 'None'}")
    print(f"Podplay clients: {', '.join(podplay_clients) if podplay_clients else 'None'}")
    
    print("\n" + "=" * 80)
    print("Required Environment Variables for GitHub Actions:")
    print("=" * 80)
    print("\n# Core Database Variables (always required):")
    print("PG_DSN")
    print("PG_SCHEMA")
    
    print("\n# CourtReserve Variables:")
    if courtreserve_clients:
        for client in courtreserve_clients:
            print(f"{client.upper()}_USERNAME")
            print(f"{client.upper()}_PASSWORD")
    else:
        print("(None - no CourtReserve customers)")
    
    print("\n# Podplay Variables:")
    if podplay_clients:
        for client in podplay_clients:
            print(f"{client.upper()}_API_KEY")
    else:
        print("(None - no Podplay customers)")
    
    print("\n# Optional Variables:")
    print("DEFAULT_LOOKBACK_DAYS  # defaults to 30 if not set")
    print("INGEST_SAMPLE_SIZE     # optional, for testing")
    
    print("\n" + "=" * 80)
    print("Note:")
    print("=" * 80)
    print("Client codes are automatically determined by querying the database.")
    print("You do NOT need to set CR_CLIENT_CODES or PODPLAY_CLIENT_CODES.")
    print("The system will query court_availability_scraper.organizations")
    print("to find all organizations where is_customer = true.")
    
except psycopg2.Error as e:
    print(f"Database error: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

