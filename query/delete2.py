import aerospike
from datetime import datetime, timedelta

AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}
NAMESPACE = "halodoc"
SET_NAME = "kv"

def delete_old_logs():
    client = aerospike.client(AERO_CONFIG).connect()

    cutoff_date = (datetime.now() - timedelta(days=180)).date()
    print(f"Menghapus log aktivitas sebelum {cutoff_date} ...")

    scan = client.scan(NAMESPACE, SET_NAME)
    keys_to_delete = []

    def callback(*args):
        """
        Callback fleksibel untuk semua versi driver:
        args bisa 3-tuple: (key, meta, bins)
        """
        try:
            # Ambil key dari tuple
            if len(args) == 3:
                key, meta, bins = args
            else:
                return

            if key is None:
                return

            user_key = key[2]  # key[2] = user_key
            if user_key and user_key.startswith("log_act:"):
                parts = user_key.split(":")
                if len(parts) == 3:
                    tanggal_str = parts[2]
                    tanggal = datetime.fromisoformat(tanggal_str).date()
                    if tanggal < cutoff_date:
                        keys_to_delete.append(key)

        except Exception as e:
            print(f"Error processing key {key if 'key' in locals() else 'Unknown'}: {e}")

    # Jalankan scan
    scan.foreach(callback)

    # Hapus semua key yang sudah dikumpulkan
    deleted_count = 0
    for k in keys_to_delete:
        try:
            client.remove(k)
            deleted_count += 1
        except Exception as e:
            print(f"Error deleting key {k}: {e}")

    print(f"{deleted_count} log aktivitas dihapus.")
    client.close()

if __name__ == "__main__":
    delete_old_logs()
