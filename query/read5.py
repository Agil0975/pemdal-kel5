import sys, os, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs

"""
Read: Menampilkan detail resep (penyakit, obat, dosis) dari suatu id janji temu pasien
"""

DB_NAME = "janji_temu"
ID_JANJI_TEMU = "JT1707"

if __name__ == "__main__":
    query = {
        "selector" : {"id_janji_temu": ID_JANJI_TEMU},
        "fields": ["id_janji_temu", "penyakit", "detail_resep"],
    }
    start = time.time()
    janji_temu = query_docs(DB_NAME, query)
    
    end = time.time()
    print("\nHasil query:\n", janji_temu)
    print(f"\nTime: {end - start} s")