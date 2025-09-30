import requests
from pymemcache.client import base
import json
from datetime import datetime, timedelta


COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")


def delete_db(db_name):
    """Delete an entire database."""
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.delete(url, auth=AUTH)

    if res.status_code == 200:
        print(f"✅ Database '{db_name}' deleted successfully")
        return True
    elif res.status_code == 404:
        print(f"ℹ️ Database '{db_name}' doesn't exist")
        return False
    else:
        print(f"❌ Failed to delete database '{db_name}': {res.text}")
        return False


def clear_all_databases():
    """Delete all project databases and recreate them fresh."""
    db_list = [
        "users",
        "janji_temu",
        "pemesanan_obat",
        "pemesanan_layanan",
        "log_aktivitas",
    ]

    print("\n--- Clearing All Databases ---")
    for db in db_list:
        delete_db(db)


# Usage
clear_all_databases()
