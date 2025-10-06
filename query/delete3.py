from datetime import datetime, timedelta
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import delete_docs_where

"""
Delete: Hapus semua janji temu yang sudah lewat lebih dari 30 hari dan masih belum selesai
"""

DB_NAME = "janji_temu"

if __name__ == "__main__":
    cutoff_date = (datetime.now() - timedelta(days=30)).isoformat()

    selector = {
        "_deleted": {"$exists": False},
        "status": {"$in": ["dijadwalkan", "sedang berlangsung", "dibatalkan"]},
        "waktu_pelaksanaan": {"$lt": cutoff_date}
    }

    print(f"Menghapus janji temu sebelum {cutoff_date} dan masih belum selesai.")
    start = time.time()
    deleted_count = delete_docs_where(DB_NAME, selector, 999999)
    print(f"Total {deleted_count} janji temu dihapus.")
    print(f"Time: {time.time() - start} s")
