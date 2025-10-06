import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
from utils.CRUDCouchDB import delete_docs_where

# === KONFIGURASI ===
DB_NAME = "janji_temu"

# 1️⃣ Tentukan batas waktu kedaluwarsa (misal: lebih dari 30 hari yang lalu)
cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()

# 2️⃣ Buat selector (kriteria pencarian Mango)
selector = {
    # "status": "dibatalkan",
    # "waktu_pelaksanaan": {"$lt": cutoff_date}
}

# 3️⃣ Jalankan penghapusan menggunakan fungsi yang sudah ada
print(f"🕒 Menghapus janji temu sebelum {cutoff_date} dengan status 'dijadwalkan'...")
deleted_count = delete_docs_where(DB_NAME, selector, 999999999)

print(f"✅ Selesai. Total {deleted_count} janji temu dihapus.")
