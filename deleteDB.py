import requests
import aerospike

COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")

AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
SET_NAME = "kv"

def delete_db_couchdb(db_name):
    """Delete an entire database."""
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.delete(url, auth=AUTH)

    if res.status_code == 200:
        print(f"Database '{db_name}' deleted successfully")
        return True
    elif res.status_code == 404:
        print(f"Database '{db_name}' doesn't exist")
        return False
    else:
        print(f"Failed to delete database '{db_name}': {res.text}")
        return False

def delete_db_aerospike():
    client = aerospike.client(AERO_CONFIG).connect()

    # Hapus semua record di set
    client.truncate(NAMESPACE, SET_NAME, 0)

    print(f"🧹 Semua data di {NAMESPACE}.{SET_NAME} telah dihapus.")
    client.close()


def clear_all_databases():
    """Delete all project databases and recreate them fresh."""
    db_list = [
        "user",
        "janji_temu",
        "pemesanan_obat",
        "pemesanan_layanan",
        "log_aktivitas",
        "obat",
        "rumah_sakit",
    ]

    print("\n--- Clearing All Databases ---")
    for db in db_list:
        delete_db_couchdb(db)
    
    delete_db_aerospike()

if __name__ == "__main__":
    clear_all_databases()
