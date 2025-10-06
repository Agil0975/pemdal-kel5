import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs
from utils.CRUDAerospike import kv_read, kv_scan

"""
Read: Menampilkan log aktivitas Baymin milik pasien tertentu.
"""

DB_NAME = "user"
EMAIL_PASIEN = 'ratihhidayanto@example.net'

if __name__ == "__main__":
    results = []
    query = {
        "selector" : {"email": EMAIL_PASIEN},
        "fields": ["nama_lengkap"],
    }
    pasien = query_docs(DB_NAME, query)[0]
    
    if not pasien:
        print("Pasien tidak ditemukan.")
        exit()
        
    records = kv_scan()
    
    for record in records:
        if record['bins']["key"].startswith("baymin:") and record['bins']["value"] == EMAIL_PASIEN:
            id_perangkat = record['bins']["key"].split(":")[1]
            
            for record2 in records:
                if record2['bins']["key"].startswith(f"log_act:{id_perangkat}"):
                    tanggal = record2['bins']["key"].split(":")[2]
                    log_aktivitas = record2['bins']["value"]
                    for jam, aktivitas in log_aktivitas.items():
                        result = {
                            "nama_lengkap": pasien["nama_lengkap"],
                            "waktu_aktivitas": f'{tanggal} {jam}',
                            "detail_aktivitas": aktivitas
                        }
                        results.append(result)

    print(results)