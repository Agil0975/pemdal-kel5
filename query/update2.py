import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.CRUDCouchDB import query_docs, update_doc, insert_docs

"""
Update: Pindahtugaskan tenaga medis ke departemen lain 
"""

DB_USER = "user"

def pindahkan_tenaga_medis(email_tm: str, id_rs_baru: str, departemen_baru: str):
    """
    Memperbarui lokasi tugas (RS dan departemen) untuk seorang tenaga medis.
    """
    # Cari tenaga medis berdasarkan email dan tipe akun
    query = {
        "selector": {
            "email": email_tm,
            "tipe_akun": "tenaga_medis"
        },
        "limit": 1
    }
    hasil_query = query_docs(DB_USER, query)

    if not hasil_query:
        print(f"Error: Tenaga medis dengan email '{email_tm}' tidak ditemukan.")
        return

    doc = hasil_query[0]
    doc_id = doc['_id']
    print(f"Profil '{doc.get('nama_lengkap')}' ditemukan.")
    print(f"Lokasi lama: RS '{doc.get('id_rs')}', Dept '{doc.get('departemen')}'\n")

    # 2. Siapkan data update dan panggil update_doc
    perubahan = {
        "id_rs": id_rs_baru,
        "departemen": departemen_baru
    }
    
    hasil_update = update_doc(DB_USER, doc_id, perubahan)

    if hasil_update:
        print(f"Sukses! Profil '{doc.get('nama_lengkap')}' telah dipindahkan.")
        print(f"Lokasi baru: RS '{id_rs_baru}', Dept '{departemen_baru}'")


if __name__ == "__main__":
    dummy = {
        "tipe_akun": "tenaga_medis",
        "email": "situmorang.adriansyah@example.net",
        "nama_lengkap": "Dr. Adriansyah Situmorang",
        "kata_sandi": "hashed123",
        "tanggal_lahir": "1985-05-15",
        "nomor_telepon": "081234567890",
        "alamat": {
            "provinsi": "Jawa Barat",
            "kota": "Bandung",
            "jalan": "Jl. Merdeka No. 10"
        },
        "NIKes": "12345678",
        "profesi": "dokter",
        "id_rs": "RS015",
        "departemen": "Umum"
    }
    insert_docs(DB_USER, dummy)
    
    email_dokter = "situmorang.adriansyah@example.net"
    rs_tujuan = "RS042"
    dept_tujuan = "Radiologi"

    start = time.time()
    pindahkan_tenaga_medis(email_dokter, rs_tujuan, dept_tujuan)
    print(f"Time: {time.time() - start} s")
    