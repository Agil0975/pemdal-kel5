import aerospike

AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
SET_NAME = "kv"

# Key yang ingin diambil
user_key = "log_act:BM998:2024-12-23"

# Buat tuple key Aerospike: (namespace, set, user_key)
key_tuple = (NAMESPACE, SET_NAME, user_key)

try:
    client = aerospike.client(AERO_CONFIG).connect()

    (key, meta, record) = client.get(key_tuple)
    print("Record ditemukan:")
    print("Key:", key)
    print("Metadata:", meta)
    print("Data:", record)

except aerospike.exception.RecordNotFound:
    print(f"Record dengan key {user_key} tidak ditemukan.")
except Exception as e:
    print("Terjadi error:", e)
finally:
    client.close()
