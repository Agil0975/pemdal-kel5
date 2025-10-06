import aerospike
import datetime
import time
from aerospike.exception import RecordNotFound

"""
Delete: Hapus semua log aktivitas Baymin yang lebih tua dari 6 bulan (sekitar 180 hari)
"""

AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
SET_NAME = "kv"

def delete_old_baymin_logs():
    client = None
    try:
        # 1. Hubungkan ke database Aerospike
        client = aerospike.client(AERO_CONFIG).connect()
        print("Berhasil terhubung ke Aerospike.")

        # 2. Tentukan tanggal batas (cutoff date)
        cutoff_date = datetime.date.today() - datetime.timedelta(days=180)
        print(f"Batas tanggal untuk penghapusan (sebelum): {cutoff_date.isoformat()}")

        # 3. Objek scan
        scan = client.scan(NAMESPACE, SET_NAME)
        
        # 4. Callback yang akan dijalankan untuk setiap record
        deleted_count = 0

        def scan_callback(record_tuple):
            nonlocal deleted_count
            
            # record_tuple berisi (key_tuple, metadata, bins_dict)
            key_tuple, _, bins = record_tuple
            
            # Ambil nilai dari bin "key"
            user_key = bins.get("key")

            # Periksa apakah ini adalah log aktivitas Baymin
            # Pastikan user_key ada, bertipe string, dan memiliki awalan yang benar
            if user_key and isinstance(user_key, str) and user_key.startswith("log_act:"):
                try:
                    # Ekstrak tanggal dari bagian akhir key
                    date_str = user_key.split(':')[-1]
                    record_date = datetime.date.fromisoformat(date_str)

                    # Bandingkan dengan tanggal batas
                    if record_date < cutoff_date:
                        # print(f"-> Menghapus log lama: {user_key} (tanggal: {record_date.isoformat()})")
                        client.remove(key_tuple)
                        deleted_count += 1

                except (ValueError, IndexError):
                    print(f"-> Melewati key dengan format tidak valid: {user_key}")
                except RecordNotFound:
                    pass

        # 5. Jalankan scan dengan callback
        scan.foreach(scan_callback)

        print(f"\nProses selesai. Total {deleted_count} log lama berhasil dihapus.")

    except Exception as e:
        print(f"Terjadi error: {e}")
    finally:
        # 6. Tutup Koneksi
        if client:
            client.close()
            print("Koneksi ke Aerospike ditutup.")


if __name__ == "__main__":
    start = time.time()
    delete_old_baymin_logs()
    print(f"Time: {time.time() - start} s")