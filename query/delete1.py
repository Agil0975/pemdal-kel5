import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import delete_docs_where

"""
Delete: Hapus semua pemesanan obat yang dibatalkan
"""

DB_NAME = "pemesanan_obat"

if __name__ == "__main__":
    selector = {
        "_deleted": {"$exists": False},
        "status_pemesanan": "dibatalkan"
    }

    print("Menghapus semua pemesanan obat dengan status 'dibatalkan'...")
    start = time.time()
    deleted_count = delete_docs_where(DB_NAME, selector, 999999)
    print(f"Total {deleted_count} dokumen dihapus dari {DB_NAME}.")
    print(f"Time: {time.time() - start} s")
