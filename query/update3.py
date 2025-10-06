import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs, update_doc

"""
Update: Ubah status suatu pemesanan layanan menjadi 'dibatalkan'
"""

DB_PEMESANAN_LAYANAN = "pemesanan_layanan"

def batalkan_pemesanan_layanan(id_pesanan: str):
    """
    Mencari pemesanan layanan berdasarkan ID dan mengubah statusnya menjadi 'dibatalkan'.
    """
    # Cari dokumen pemesanan layanan
    query = {
        "selector": {
            "id_pesanan": id_pesanan
        }, 
        "limit": 1
    }
    hasil_query = query_docs(DB_PEMESANAN_LAYANAN, query)

    if not hasil_query:
        print(f"Error: Pemesanan layanan dengan ID '{id_pesanan}' tidak ditemukan.")
        return

    doc = hasil_query[0]
    doc_id = doc['_id']
    status_sekarang = doc.get('status_pemesanan')

    print(f"Pemesanan ditemukan. Status saat ini: '{status_sekarang}'")
    
    if status_sekarang == "dibatalkan":
        print("Pemesanan ini sudah dalam status 'dibatalkan'. Tidak ada perubahan.")
        return

    # Siapkan data update dan panggil update_doc
    perubahan = {"status_pemesanan": "dibatalkan"}
    hasil_update = update_doc(DB_PEMESANAN_LAYANAN, doc_id, perubahan)

    if hasil_update:
        print(f"Sukses! Pemesanan layanan '{id_pesanan}' telah dibatalkan.")

if __name__ == "__main__":
    id_layanan_untuk_dibatalkan = "PL001"
    
    start = time.time() 
    batalkan_pemesanan_layanan(id_layanan_untuk_dibatalkan)
    print(f"Time: {time.time() - start} s")