import aerospike
import datetime
from aerospike.exception import RecordNotFound

# ==========================
# KONFIGURASI
# ==========================
AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
SET_NAME = "kv"

# ==========================
# FUNGSI UTAMA
# ==========================
def delete_old_baymin_logs():
    """
    Menghubungkan ke Aerospike, memindai, dan menghapus log aktivitas Baymin
    yang lebih tua dari 6 bulan (sekitar 180 hari).
    """
    client = None
    try:
        # 1. Hubungkan ke database Aerospike
        client = aerospike.client(AERO_CONFIG).connect()
        print("Berhasil terhubung ke Aerospike.")

        # 2. Tentukan tanggal batas (cutoff date)
        # Kita anggap 6 bulan adalah sekitar 180 hari
        cutoff_date = datetime.date.today() - datetime.timedelta(days=180)
        print(f"Batas tanggal untuk penghapusan (sebelum): {cutoff_date.isoformat()}")

        # 3. Siapkan objek scan
        scan = client.scan(NAMESPACE, SET_NAME)
        
        # 4. Definisikan callback yang akan dijalankan untuk setiap record
        #    Callback ini akan berisi logika untuk memeriksa dan menghapus record
        deleted_count = 0

        def scan_callback(record_tuple):
            nonlocal deleted_count
            _, _, bins = record_tuple
            # user_key = bins.get["key"] # Ambil user_key dari tuple (ns, set, key, digest)
            print(bins.get("key", None))
            deleted_count += 1
            # # Periksa apakah ini adalah log aktivitas Baymin
            # if user_key and user_key.startswith("log_act:"):
            #     try:
            #         # Ekstrak tanggal dari bagian akhir key
            #         date_str = user_key.split(':')[-1]
            #         record_date = datetime.date.fromisoformat(date_str)

            #         # Bandingkan dengan tanggal batas
            #         if record_date < cutoff_date:
            #             print(f"-> Menghapus log lama: {user_key} (tanggal: {record_date.isoformat()})")
            #             client.remove(key)
            #             deleted_count += 1

            #     except (ValueError, IndexError):
            #         # Tangani jika format key tidak sesuai atau tanggal tidak valid
            #         print(f"-> Melewati key dengan format tidak valid: {user_key}")
            #     except RecordNotFound:
            #         # Record mungkin sudah dihapus oleh proses lain
            #         pass
        # 5. Jalankan scan dengan callback
        #    Kita harus menyertakan policy `send_key: True` agar user_key bisa dibaca
        scan_policy = {'send_key': True}
        scan.foreach(scan_callback, policy=scan_policy)
        print(deleted_count)
        

        print(f"\nProses selesai. Total {deleted_count} log lama berhasil dihapus.")

    except Exception as e:
        print(f"Terjadi error: {e}")
    finally:
        # 6. Pastikan koneksi ditutup
        if client:
            client.close()
            print("Koneksi ke Aerospike ditutup.")

# ==========================
# EKSEKUSI SKRIP
# ==========================
if __name__ == "__main__":
    delete_old_baymin_logs()