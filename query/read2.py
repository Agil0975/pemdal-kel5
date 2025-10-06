import sys, os, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import create_index, query_docs

"""
Read: Menampilkan daftar obat dengan stok tersisa kurang dari 20
"""

DB_NAME = "obat"

if __name__ == "__main__":
    create_index(DB_NAME, ["stok"], "idx_stok")
    query = {
        "selector" : {"stok": {"$lt": 20}},
        "fields": ["id_obat", "label", "stok"],
        "sort": [{"stok": "asc"}]
    }
    start = time.time()
    obat_list = query_docs(DB_NAME, query)
    
    end = time.time()
    print("\nHasil query:\n", obat_list)
    print(f"\nTime: {end - start} s")