import aerospike
import json
import time

# KONFIGURASI KONEKSI
AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "test"
SET_NAME = "kv"

try:
    aero_client = aerospike.client(AERO_CONFIG).connect()
    print("Terhubung ke Aerospike.")
except Exception as e:
    print(f"Gagal terhubung ke Aerospike: {e}")
    aero_client = None


# ======================================================
# CREATE / INSERT
# ======================================================
def kv_insert(key, value):
    """
    Insert one key-value pair into Aerospike.
    
    Args:
        key (str): the key to insert
        value (any): the value to insert
        
    Returns:
        bool: True if insert successful, False otherwise
        
    Example:
        kv_insert("user_001", {"name": "Alice", "age": 30})
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.put((NAMESPACE, SET_NAME, key), {"value": value})
        print(f"Insert ke KV (key: {key}) berhasil.")
        return True
    except Exception as e:
        print(f"Insert gagal untuk key {key}: {e}")
        return False


# ======================================================
# READ
# ======================================================
def kv_read(key):
    """
    Retrieve value by key from Aerospike.

    Args:
        key (str): the key to read

    Returns:
        any: the value associated with the key, or None if not found
        
    Example:
        value = kv_read("user_001")
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return None

    try:
        (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
        value = record.get("value") if record else None
        print(f"Baca key '{key}': {value}")
        return value
    except aerospike.exception.RecordNotFound:
        print(f"Key '{key}' tidak ditemukan.")
        return None
    except Exception as e:
        print(f"Gagal membaca key {key}: {e}")
        return None


# ======================================================
# UPDATE
# ======================================================
def kv_update(key, new_value):
    """
    Update the value of an existing key in Aerospike.
    
    Args:
        key (str): the key to update
        new_value (any): the new value to set
    
    Returns:
        bool: True if update successful, False otherwise
        
    Example:
        kv_update("user_001", {"name": "Alice", "age": 31})
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.get((NAMESPACE, SET_NAME, key))
        aero_client.put((NAMESPACE, SET_NAME, key), {"value": new_value})
        print(f"Key '{key}' berhasil diperbarui.")
        return True
    except aerospike.exception.RecordNotFound:
        print(f"Key '{key}' tidak ditemukan. Gunakan insert untuk menambah baru.")
        return False
    except Exception as e:
        print(f"Update gagal untuk key {key}: {e}")
        return False


# ======================================================
# DELETE
# ======================================================
def kv_delete(key):
    """
    Delete a key-value pair from Aerospike by key.
    
    Args:
        key (str): the key to delete
        
    Returns:
        bool: True if delete successful, False otherwise
        
    Example:
        kv_delete("user_001")
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.remove((NAMESPACE, SET_NAME, key))
        print(f"Key '{key}' berhasil dihapus.")
        return True
    except aerospike.exception.RecordNotFound:
        print(f"Key '{key}' tidak ditemukan.")
        return False
    except Exception as e:
        print(f"Gagal menghapus key {key}: {e}")
        return False


# ======================================================
# BULK INSERT
# ======================================================
def kv_bulk_insert(data_dict):
    """
    Insert many key-value pairs into Aerospike.
    
    Args:
        data_dict (dict): pasangan key-value
    
    Returns:
        int: jumlah insert sukses
    
    Example:
        kv_bulk_insert({"user_001": {"name": "Alice"}, "user_002": {"name": "Bob"}})    
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0
    for key, value in data_dict.items():
        try:
            aero_client.put((NAMESPACE, SET_NAME, key), {"value": value})
            success += 1
        except Exception as e:
            print(f"Gagal insert {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"Bulk insert {success}/{len(data_dict)} key selesai dalam {dur:.2f} ms.")
    return success


# ======================================================
# BULK READ
# ======================================================
def kv_bulk_read(keys):
    """
    Retrieve many values by their keys from Aerospike.
    
    Args:
        keys (list): daftar key
    
    Returns:
        dict: pasangan key-value (None jika tidak ditemukan)
        
    Example:
        results = kv_bulk_read(["user_001", "user_002"])
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return {}

    start = time.time()
    results = {}

    for key in keys:
        try:
            (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
            results[key] = record.get("value") if record else None
        except aerospike.exception.RecordNotFound:
            results[key] = None
        except Exception as e:
            print(f"Gagal baca key {key}: {e}")
            results[key] = None

    dur = (time.time() - start) * 1000
    print(f"Bulk read {len(keys)} key selesai dalam {dur:.2f} ms.")
    return results


# ======================================================
# BULK UPDATE
# ======================================================
def kv_bulk_update(data_dict):
    """
    Update many key-value pairs in Aerospike.
    
    Args:
        data_dict (dict): pasangan key-value baru
        
    Returns:
        int: jumlah update sukses
        
    Example:
        kv_bulk_update({"user_001": {"name": "Alice", "age": 30}, "user_002": {"name": "Bob", "age": 25}})
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0

    for key, new_value in data_dict.items():
        try:
            aero_client.get((NAMESPACE, SET_NAME, key))
            aero_client.put((NAMESPACE, SET_NAME, key), {"value": new_value})
            success += 1
        except aerospike.exception.RecordNotFound:
            print(f"Key '{key}' tidak ditemukan (skip).")
        except Exception as e:
            print(f"Gagal update {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"Bulk update {success}/{len(data_dict)} key selesai dalam {dur:.2f} ms.")
    return success


# ======================================================
# BULK DELETE
# ======================================================
def kv_bulk_delete(keys):
    """
    Delete many key-value pairs from Aerospike by their keys.
    
    Args:
        keys (list): daftar keys to delete
    
    Returns:
        int: jumlah delete sukses
        
    Example:
        kv_bulk_delete(["user_001", "user_002"])
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0

    for key in keys:
        try:
            aero_client.remove((NAMESPACE, SET_NAME, key))
            success += 1
        except aerospike.exception.RecordNotFound:
            print(f"Key '{key}' tidak ditemukan (skip).")
        except Exception as e:
            print(f"Gagal hapus {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"Bulk delete {success}/{len(keys)} key selesai dalam {dur:.2f} ms.")
    return success
