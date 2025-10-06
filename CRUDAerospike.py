import aerospike
import json
import time

# --- KONFIGURASI KONEKSI ---
AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "test"
SET_NAME = "kv"

try:
    aero_client = aerospike.client(AERO_CONFIG).connect()
    print("✅ Terhubung ke Aerospike.")
except Exception as e:
    print(f"❌ Gagal terhubung ke Aerospike: {e}")
    aero_client = None


# ======================================================
# CREATE / INSERT
# ======================================================
def kv_insert(key, value):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.put((NAMESPACE, SET_NAME, key), {"value": value})
        print(f"✅ Insert ke KV (key: {key}) berhasil.")
        return True
    except Exception as e:
        print(f"❌ Insert gagal untuk key {key}: {e}")
        return False


# ======================================================
# READ
# ======================================================
def kv_read(key):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return None

    try:
        (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
        value = record.get("value") if record else None
        print(f"📖 Baca key '{key}': {value}")
        return value
    except aerospike.exception.RecordNotFound:
        print(f"ℹ️ Key '{key}' tidak ditemukan.")
        return None
    except Exception as e:
        print(f"❌ Gagal membaca key {key}: {e}")
        return None


# ======================================================
# UPDATE
# ======================================================
def kv_update(key, new_value):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.get((NAMESPACE, SET_NAME, key))
        aero_client.put((NAMESPACE, SET_NAME, key), {"value": new_value})
        print(f"♻️ Key '{key}' berhasil diperbarui.")
        return True
    except aerospike.exception.RecordNotFound:
        print(f"⚠️ Key '{key}' tidak ditemukan. Gunakan insert untuk menambah baru.")
        return False
    except Exception as e:
        print(f"❌ Update gagal untuk key {key}: {e}")
        return False


# ======================================================
# DELETE
# ======================================================
def kv_delete(key):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.remove((NAMESPACE, SET_NAME, key))
        print(f"🗑️ Key '{key}' berhasil dihapus.")
        return True
    except aerospike.exception.RecordNotFound:
        print(f"ℹ️ Key '{key}' tidak ditemukan.")
        return False
    except Exception as e:
        print(f"❌ Gagal menghapus key {key}: {e}")
        return False


# ======================================================
# BULK INSERT
# ======================================================
def kv_bulk_insert(data_dict):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0
    for key, value in data_dict.items():
        try:
            aero_client.put((NAMESPACE, SET_NAME, key), {"value": value})
            success += 1
        except Exception as e:
            print(f"❌ Gagal insert {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"✅ Bulk insert {success}/{len(data_dict)} key selesai dalam {dur:.2f} ms.")
    return success


# ======================================================
# BULK READ
# ======================================================
def kv_bulk_read(keys):
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
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
            print(f"❌ Gagal baca key {key}: {e}")
            results[key] = None

    dur = (time.time() - start) * 1000
    print(f"📦 Bulk read {len(keys)} key selesai dalam {dur:.2f} ms.")
    return results


# ======================================================
# BULK UPDATE
# ======================================================
def kv_bulk_update(data_dict):
    """
    Memperbarui banyak key sekaligus.
    Args:
        data_dict (dict): pasangan key-new_value
    Returns:
        int: jumlah update sukses
    """
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0

    for key, new_value in data_dict.items():
        try:
            aero_client.get((NAMESPACE, SET_NAME, key))
            aero_client.put((NAMESPACE, SET_NAME, key), {"value": new_value})
            success += 1
        except aerospike.exception.RecordNotFound:
            print(f"⚠️ Key '{key}' tidak ditemukan (skip).")
        except Exception as e:
            print(f"❌ Gagal update {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"♻️ Bulk update {success}/{len(data_dict)} key selesai dalam {dur:.2f} ms.")
    return success


# ======================================================
# BULK DELETE
# ======================================================
def kv_bulk_delete(keys):
    """
    Menghapus banyak key sekaligus.
    Args:
        keys (list): daftar key
    Returns:
        int: jumlah delete sukses
    """
    if not aero_client:
        print("❌ Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0

    for key in keys:
        try:
            aero_client.remove((NAMESPACE, SET_NAME, key))
            success += 1
        except aerospike.exception.RecordNotFound:
            print(f"ℹ️ Key '{key}' tidak ditemukan (skip).")
        except Exception as e:
            print(f"❌ Gagal hapus {key}: {e}")

    dur = (time.time() - start) * 1000
    print(f"🗑️ Bulk delete {success}/{len(keys)} key selesai dalam {dur:.2f} ms.")
    return success
