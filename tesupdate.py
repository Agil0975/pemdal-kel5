import requests

COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")

# Ambil dokumen dulu
doc_id = "user_001"
res = requests.get(f"{COUCHDB_URL}/users/{doc_id}", auth=AUTH)
doc = res.json()

# Update field role
doc["role"] = "premium_customer"

# Kirim update (wajib sertakan _rev)
res = requests.put(f"{COUCHDB_URL}/users/{doc_id}", json=doc, auth=AUTH)
print(res.json())
