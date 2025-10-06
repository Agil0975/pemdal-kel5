import sys, os, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs
from utils.CRUDAerospike import kv_read

"""
Read: Menampilkan tenaga medis dan berapa banyak janji temu yang mereka tangani
"""

DB_NAME = "user"

if __name__ == "__main__":
    query = {
        "selector" : {"tipe_akun": "tenaga_medis"},
        "fields": ["nama_lengkap", "email", "profesi"],
    }
    start = time.time()
    tendik_list = query_docs(DB_NAME, query)
    
    for tendik in tendik_list:
        key_email = tendik["email"]
        total_janji_temu = kv_read(f"tenaga_medis:{key_email}:janji_temu")
        tendik["jumlah_janji_temu"] = len(total_janji_temu) if total_janji_temu else 0
        
    end = time.time()
    print("\nHasil query:\n", tendik_list)
    print(f"\nTime: {end - start} s")