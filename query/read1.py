import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs
from utils.CRUDAerospike import kv_read

"""
Read: Menampilkan daftar pasien dan jumlah total pesanan obat yang pernah mereka buat
"""

DB_NAME = "user"

if __name__ == "__main__":
    query = {
        "selector" : {"tipe_akun": "pasien"},
        "fields": ["nama_lengkap", "email"],
    }
    pasien_list = query_docs(DB_NAME, query)
    
    for pasien in pasien_list:
        key_email = pasien["email"]
        total_pemesanan_obat = kv_read(f"user:{key_email}:pemesanan_obat")
        pasien["total_pesanan"] = len(total_pemesanan_obat) if total_pemesanan_obat else 0
        
    print(pasien_list)