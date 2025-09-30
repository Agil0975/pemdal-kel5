import requests
from pymemcache.client import base
import json
from datetime import datetime, timedelta

# Tes CouchDB
COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")


def query_docs(db_name, query):
    """
    Query CouchDB database with a Mango query.

    Args:
        db_name (str): Name of the database to query
        query (dict): Mango query as Python dictionary

    Returns:
        list: List of documents matching the query, or empty list if error

    Example:
        query = {
            "selector": {"tipe_akun": "pasien"},
            "limit": 10,
            "sort": [{"nama_lengkap": "asc"}]
        }
        patients = query_docs("users", query)
    """
    url = f"{COUCHDB_URL}/{db_name}/_find"
    res = requests.post(url, json=query, auth=AUTH)

    if res.status_code == 200:
        return res.json()["docs"]
    else:
        print(f"❌ Query gagal pada {db_name}: {res.text}")
        return []


def insert_docs(db_name, docs):
    """
    Insert one or multiple documents into CouchDB.

    Args:
        db_name (str): Name of the database
        docs (dict or list): Single document (dict) or multiple documents (list of dicts)

    Returns:
        dict or list: Response from CouchDB with _id and _rev, or None if failed

    Examples:
        # Insert single document
        result = insert_docs("users", {
            "_id": "user123",
            "nama": "Ahmad",
            "email": "ahmad@mail.com"
        })

        # Insert multiple documents
        results = insert_docs("users", [
            {"_id": "user1", "nama": "Ahmad"},
            {"_id": "user2", "nama": "Budi"},
            {"_id": "user3", "nama": "Citra"}
        ])
    """
    # Handle single document
    if isinstance(docs, dict):
        url = f"{COUCHDB_URL}/{db_name}"
        res = requests.post(url, json=docs, auth=AUTH)

        if res.status_code == 201:
            print(f"✅ Insert ke {db_name} (ID: {docs.get('_id', 'auto')}): Sukses")
            return res.json()
        elif res.status_code == 409:  # Conflict
            print(
                f"⚠️ Insert ke {db_name} (ID: {docs.get('_id', 'auto')}): Conflict (ID sudah ada)"
            )
            return None
        else:
            print(
                f"❌ Insert ke {db_name} (ID: {docs.get('_id', 'auto')}): Gagal - {res.json()}"
            )
            return None

    # Handle multiple documents (bulk insert)
    elif isinstance(docs, list):
        url = f"{COUCHDB_URL}/{db_name}/_bulk_docs"
        payload = {"docs": docs}
        res = requests.post(url, json=payload, auth=AUTH)

        if res.status_code in [201, 202]:
            results = res.json()
            success_count = sum(1 for r in results if r.get("ok"))
            error_count = len(results) - success_count
            print(
                f"✅ Bulk insert ke {db_name}: {success_count} sukses, {error_count} gagal"
            )
            return results
        else:
            print(f"❌ Bulk insert ke {db_name} gagal: {res.text}")
            return None

    else:
        print(f"❌ Invalid input type. Expected dict or list, got {type(docs)}")
        return None


def delete_docs_where(db_name, selector, limit=None):
    """
    Delete documents matching a condition (like SQL DELETE WHERE).

    Args:
        db_name (str): Name of the database
        selector (dict): Mango selector (conditions for deletion)
        limit (int, optional): Maximum number of documents to delete

    Returns:
        int: Number of documents deleted

    Examples:
        # Delete old logs
        delete_docs_where("log_aktivitas", {
            "waktu_aktivitas": {"$lt": "2025-09-01"}
        })

        # Delete inactive users
        delete_docs_where("users", {
            "status": "inactive"
        })

        # Delete with limit (safety!)
        delete_docs_where("pemesanan_obat", {
            "status_pemesanan": "dibatalkan"
        }, limit=100)
    """
    # Step 1: Query to find matching documents
    query = {"selector": selector}
    if limit:
        query["limit"] = limit

    docs_to_delete = query_docs(db_name, query)

    if not docs_to_delete:
        print(f"ℹ️ Tidak ada dokumen yang cocok dengan kriteria di {db_name}")
        return 0

    print(f"🔍 Ditemukan {len(docs_to_delete)} dokumen untuk dihapus dari {db_name}")

    # Step 2: Prepare bulk delete (mark with _deleted: true)
    delete_payload = []
    for doc in docs_to_delete:
        delete_payload.append(
            {"_id": doc["_id"], "_rev": doc["_rev"], "_deleted": True}
        )

    # Step 3: Execute bulk delete
    url = f"{COUCHDB_URL}/{db_name}/_bulk_docs"
    payload = {"docs": delete_payload}
    res = requests.post(url, json=payload, auth=AUTH)

    if res.status_code in [201, 202]:
        results = res.json()
        success_count = sum(1 for r in results if r.get("ok"))
        print(f"✅ Berhasil menghapus {success_count} dokumen dari {db_name}")
        return success_count
    else:
        print(f"❌ Gagal menghapus dokumen dari {db_name}: {res.text}")
        return 0


def create_index(db_name, fields, index_name=None):
    """
    Create an index on specified fields for better query performance.

    Args:
        db_name (str): Name of the database
        fields (list): List of fields to index (e.g., ["nama_lengkap"] or ["status", "kota"])
        index_name (str, optional): Custom name for the index

    Returns:
        bool: True if successful

    Example:
        create_index("users", ["nama_lengkap"])
        create_index("users", ["status", "alamat.kota"], "idx_status_city")
    """
    url = f"{COUCHDB_URL}/{db_name}/_index"

    index_def = {"index": {"fields": fields}, "type": "json"}

    if index_name:
        index_def["name"] = index_name

    res = requests.post(url, json=index_def, auth=AUTH)

    if res.status_code in [200, 201]:
        print(f"✅ Index created on {db_name}: {fields}")
        return True
    else:
        print(f"❌ Failed to create index on {db_name}: {res.text}")
        return False


# Helper function to create database
def create_db(db_name):
    """Membuat database baru jika belum ada."""
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.put(url, auth=AUTH)
    if res.status_code == 201:
        print(f"Database '{db_name}' dibuat.")
    elif res.status_code == 412:
        print(f"Database '{db_name}' sudah ada.")
    else:
        print(f"Gagal membuat database {db_name}: {res.text}")
