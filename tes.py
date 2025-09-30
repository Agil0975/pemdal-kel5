from pymemcache.client import base
import requests

# Tes Memcached
client = base.Client(('127.0.0.1', 11211))
client.set("foo", "bar")
client.set("number", 42)
client.set("employee:1:name", "Alice")
client.set("employee:1:age", 30)
client.set("employee:2:name", "Bob")
client.set("employee:2:age", 25)
print("Memcached get:", client.get("foo"))
print("Memcached get number:", client.get("number"))
print("Memcached get employee 1 name:", client.get("employee:1:name"))
print("Memcached get employee 2 age:", client.get("employee:2:age"))


# Tes CouchDB
COUCHDB_URL = "http://127.0.0.1:5984"
res = requests.get(COUCHDB_URL, auth=("admin", "admin123"))
print("CouchDB info:", res.json())
AUTH = ("admin", "admin123")  # sesuaikan dengan user CouchDB kamu

def create_db(db_name):
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.put(url, auth=AUTH)
    if res.status_code in (201, 412):  # 201 = created, 412 = already exists
        print(f"Database '{db_name}' siap dipakai.")
    else:
        print(f"Gagal membuat database {db_name}: {res.text}")

def insert_doc(db_name, doc):
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.post(url, json=doc, auth=AUTH)
    print(f"Insert ke {db_name}: {res.json()}")

# --- Buat database ---
create_db("users")
create_db("products")

# --- Isi data users ---
users = [
    {"_id": "user_001", "name": "Agil", "email": "agil@example.com", "role": "customer"},
    {"_id": "user_002", "name": "Budi", "email": "budi@example.com", "role": "admin"},
    {"_id": "user_003", "name": "Citra", "email": "citra@example.com", "role": "seller"},
]

for u in users:
    insert_doc("users", u)

# --- Isi data products ---
products = [
    {"_id": "prd_001", "name": "Laptop", "price": 12000000, "stock": 5},
    {"_id": "prd_002", "name": "Mouse", "price": 150000, "stock": 20},
    {"_id": "prd_003", "name": "Keyboard", "price": 300000, "stock": 10},
]

for p in products:
    insert_doc("products", p)

print("✅ Semua data berhasil dimasukkan ke CouchDB.")
