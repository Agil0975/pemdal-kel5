import aerospike
import json
import time

# KONFIGURASI KONEKSI
AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
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
    Menyisipkan satu pasangan key-value ke Aerospike dengan skema baru.
    Record akan berisi: {"key": key, "value": value}
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return False

    try:
        record_bins = {"key": key, "value": value}
        aero_client.put((NAMESPACE, SET_NAME, key), record_bins)
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
    Mengambil seluruh record (termasuk key dan value) berdasarkan key.
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return None

    try:
        (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
        print(f"Baca key '{key}': {record}")
        return record
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
    Memperbarui bin 'value' dari sebuah key yang sudah ada.
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return False

    try:
        aero_client.get((NAMESPACE, SET_NAME, key))
        record_bins = {"key": key, "value": new_value}
        aero_client.put((NAMESPACE, SET_NAME, key), record_bins)
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
    Menghapus pasangan key-value dari Aerospike berdasarkan key.
    (Tidak perlu diubah, karena 'remove' bekerja pada primary key)
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
    Menyisipkan banyak pasangan key-value ke Aerospike.
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0
    for key, value in data_dict.items():
        try:
            record_bins = {"key": key, "value": value}
            aero_client.put((NAMESPACE, SET_NAME, key), record_bins)
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
    Mengambil banyak record berdasarkan daftar key.
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return {}

    start = time.time()
    results = {}

    key_tuples = [(NAMESPACE, SET_NAME, k) for k in keys]
    records = aero_client.get_many(key_tuples)
    
    for key_tuple, record_data in records:
        key_str = key_tuple[2]
        if record_data:
            # record_data[2] adalah kamus bin
            results[key_str] = record_data[2]
        else:
            results[key_str] = None

    dur = (time.time() - start) * 1000
    print(f"Bulk read {len(keys)} key selesai dalam {dur:.2f} ms.")
    return results


# ======================================================
# BULK UPDATE
# ======================================================
def kv_bulk_update(data_dict):
    """
    Memperbarui banyak pasangan key-value di Aerospike.
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0

    for key, new_value in data_dict.items():
        try:
            ops = [
                aerospike.operations.write("value", new_value)
            ]
            aero_client.operate((NAMESPACE, SET_NAME, key), ops)
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
    Menghapus banyak pasangan key-value dari Aerospike.
    (Tidak perlu diubah, tapi bisa dibuat lebih efisien)
    """
    if not aero_client:
        print("Koneksi Aerospike belum siap.")
        return 0

    start = time.time()
    success = 0
    
    key_tuples = [(NAMESPACE, SET_NAME, k) for k in keys]
    results = aero_client.remove_many(key_tuples)
    
    for key_tuple, result_code in results:
        key_str = key_tuple[2]
        if result_code == aerospike.OK:
            success += 1
        elif result_code == aerospike.ERR_RECORD_NOT_FOUND:
            print(f"Key '{key_str}' tidak ditemukan (skip).")
        else:
            print(f"Gagal hapus {key_str} dengan kode error: {result_code}")

    dur = (time.time() - start) * 1000
    print(f"Bulk delete {success}/{len(keys)} key selesai dalam {dur:.2f} ms.")
    return success