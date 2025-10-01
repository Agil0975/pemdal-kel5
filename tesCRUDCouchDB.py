import requests
from pymemcache.client import base
import json
from datetime import datetime

# Tes CouchDB
COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")  # sesuaikan dengan user CouchDB kamu
# res = requests.get(COUCHDB_URL, auth=AUTH)
# print("CouchDB info:", res.json())

def create_db(db_name):
    """Membuat database baru jika belum ada."""
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.put(url, auth=AUTH)
    if res.status_code == (201):  # 201 = created, 412 = already exists
        print(f"Database '{db_name}' dibuat.")
        pass
    elif res.status_code == 412:
        print(f"Database '{db_name}' sudah ada, tidak dibuat ulang.")
        pass
    else:
        print(f"Gagal membuat database {db_name}: {res.text}")

def insert_doc(db_name, doc):
    """Menyisipkan/Memperbarui dokumen. Jika _id sudah ada, CouchDB akan menolaknya.
       Gunakan update_doc untuk memperbarui dokumen yang sudah ada."""
    url = f"{COUCHDB_URL}/{db_name}"
    res = requests.post(url, json=doc, auth=AUTH)
    if res.status_code == 201:
        print(f"✅ Insert ke {db_name} (ID: {doc.get('_id', 'tanpa_id')}): Sukses")
        pass
    elif 'conflict' in res.json().get('reason', ''):
        print(f"⚠️ Insert ke {db_name} (ID: {doc.get('_id', 'tanpa_id')}): Conflict (ID sudah ada)")
        pass
    else:
        print(f"❌ Insert ke {db_name} (ID: {doc.get('_id', 'tanpa_id')}): Gagal - {res.json()}")

# --- Fungsi CRUD Tambahan ---

def get_doc(db_name, doc_id):
    """Membaca dokumen berdasarkan _id."""
    url = f"{COUCHDB_URL}/{db_name}/{doc_id}"
    res = requests.get(url, auth=AUTH)
    if res.status_code == 200:
        return res.json()
    elif res.status_code == 404:
        print(f"Dokumen {doc_id} di {db_name} tidak ditemukan.")
        return None
    else:
        print(f"Gagal membaca dokumen {doc_id} di {db_name}: {res.text}")
        return None

def update_doc(db_name, doc_id, new_data):
    """Memperbarui dokumen yang sudah ada. Membutuhkan _rev."""
    existing_doc = get_doc(db_name, doc_id)
    if existing_doc:
        # Gabungkan data baru ke data yang sudah ada, termasuk _rev
        updated_doc = existing_doc.copy()
        updated_doc.update(new_data)
        
        # Pastikan _id dan _rev ada di payload
        updated_doc['_id'] = doc_id
        if '_rev' not in updated_doc:
            updated_doc['_rev'] = existing_doc['_rev']
            
        url = f"{COUCHDB_URL}/{db_name}/{doc_id}"
        res = requests.put(url, json=updated_doc, auth=AUTH)
        if res.status_code == 201:
            print(f"✅ Update dokumen {doc_id} di {db_name}: Sukses")
            return res.json()
        else:
            print(f"❌ Gagal update dokumen {doc_id} di {db_name}: {res.text}")
            return None
    else:
        print(f"⚠️ Dokumen {doc_id} di {db_name} tidak dapat diupdate karena tidak ditemukan.")
        return None

def delete_doc(db_name, doc_id):
    """Menghapus dokumen. Membutuhkan _rev."""
    existing_doc = get_doc(db_name, doc_id)
    if existing_doc:
        rev = existing_doc.get('_rev')
        url = f"{COUCHDB_URL}/{db_name}/{doc_id}?rev={rev}"
        res = requests.delete(url, auth=AUTH)
        if res.status_code == 200:
            print(f"✅ Delete dokumen {doc_id} di {db_name}: Sukses")
            return True
        else:
            print(f"❌ Gagal delete dokumen {doc_id} di {db_name}: {res.text}")
            return False
    else:
        print(f"⚠️ Dokumen {doc_id} di {db_name} tidak dapat dihapus karena tidak ditemukan.")
        return False

# --- Data Seeding (Contoh Data Awal) ---

print("\n" + "="*50)
print("              START COUCHDB CRUD DEMO")
print("="*50)

# Daftar Database yang akan dibuat
db_list = ["users", "janji_temu", "pemesanan_obat", "pemesanan_layanan", "log_aktivitas"]
for db in db_list:
    create_db(db)

# 1. Dokumen User
print("\n--- Seeding Users ---")
users_data = [
    {
        "_id": "pasien_a@mail.com",
        "kata_sandi": "hashed_pass_1",
        "nama_lengkap": "Ahmad Pasien",
        "tanggal_lahir": "1990-01-15",
        "nomor_telepon": "081211112222",
        "alamat": {"provinsi": "Jawa Barat", "kota": "Bandung", "jalan": "Jl. Merdeka No. 10"},
        "tipe_akun": "pasien",
    },
    {
        "_id": "medis_b@mail.com",
        "kata_sandi": "hashed_pass_2",
        "nama_lengkap": "Dr. Bunga Tenaga Medis",
        "tanggal_lahir": "1985-05-20",
        "nomor_telepon": "081333334444",
        "alamat": {"provinsi": "DKI Jakarta", "kota": "Jakarta Pusat", "jalan": "Jl. Sudirman No. 5"},
        "tipe_akun": "tenaga_medis",
        "NIKes": "1234567890",
        "profesi": "Dokter Umum",
        "id_rs": "RSU001",
        "nama_departemen": "Poli Umum",
    },
]
for user in users_data:
    insert_doc("users", user)

# 2. Dokumen Janji Temu
print("\n--- Seeding Janji Temu ---")
janji_temu_data = [
    {
        "_id": "jt_001",
        "waktu_pelaksanaan": datetime(2025, 11, 15, 10, 0, 0).isoformat(),
        "alasan": "Sakit kepala dan demam ringan",
        "email_pasien": "pasien_a@mail.com",
        "email_tenaga_medis": "medis_b@mail.com",
        "rs": {
            "nama_rs": "RS Utama Jaya", "email": "rsujaya@mail.com", "no_telepon": "02277778888",
            "provinsi": "Jawa Barat", "kota": "Bandung", "jalan": "Jl. Raya Timur"
        },
        "penyakit": "Flu",
        "detail_resep": [
            {"id_obat": "OBT001", "dosis": "3x sehari", "label_obat": "Paracetamol 500mg"}
        ],
    }
]
for jt in janji_temu_data:
    insert_doc("janji_temu", jt)

# 3. Dokumen Pemesanan Obat
print("\n--- Seeding Pemesanan Obat ---")
pemesanan_obat_data = [
    {
        "_id": "po_001",
        "email_pemesan": "pasien_a@mail.com",
        "waktu_pemesanan": datetime.now().isoformat(),
        "status_pemesanan": "Diproses",
        "detail_pesanan": [
            {"id_obat": "OBT001", "jumlah": 1, "harga_satuan": 50000, "label_obat": "Paracetamol 500mg"},
            {"id_obat": "OBT002", "jumlah": 2, "harga_satuan": 75000, "label_obat": "Vitamin C"}
        ],
    }
]
for po in pemesanan_obat_data:
    insert_doc("pemesanan_obat", po)

# 4. Dokumen Pemesanan Layanan
print("\n--- Seeding Pemesanan Layanan ---")
pemesanan_layanan_data = [
    {
        "_id": "pl_001",
        "email_pemesan": "pasien_a@mail.com",
        "waktu_pemesanan": datetime.now().isoformat(),
        "jadwal_pelaksanaan": datetime(2025, 11, 16, 14, 0, 0).isoformat(),
        "status_pemesanan": "Terjadwal",
        "rs": {
            "nama_rs": "RS Utama Jaya", "email": "rsujaya@mail.com", "no_telepon": "02277778888",
            "provinsi": "Jawa Barat", "kota": "Bandung", "jalan": "Jl. Raya Timur"
        },
        "layanan": {"nama_layanan": "Fisioterapi", "biaya": 250000},
    }
]
for pl in pemesanan_layanan_data:
    insert_doc("pemesanan_layanan", pl)

# 5. Dokumen Log Aktivitas
print("\n--- Seeding Log Aktivitas ---")
log_aktivitas_data = [
    {
        "_id": "log_001",
        "id_perangkat": "BAYMIN_DEV_001",
        "waktu_aktivitas": datetime.now().isoformat(),
        "detail_aktivitas": "Pengukuran suhu tubuh, hasil: 37.0 C",
        "email_pasien": "pasien_a@mail.com",
    }
]
for log in log_aktivitas_data:
    insert_doc("log_aktivitas", log)

print("\n" + "="*50)
print("       DEMO CRUD PADA COUCHDB")
print("="*50)

# --- Contoh Penggunaan CRUD ---

## 1. CREATE (sudah dilakukan saat Seeding)

## 2. READ (Membaca)
print("\n--- READ: Ambil Dokumen User 'medis_b@mail.com' ---")
doc_id_to_read = "medis_b@mail.com"
read_result = get_doc("users", doc_id_to_read)
if read_result:
    print(f"Dokumen ditemukan:\n{json.dumps(read_result, indent=2)}")


## 3. UPDATE (Memperbarui)
print("\n--- UPDATE: Ubah Nomor Telepon dan Profesi User 'medis_b@mail.com' ---")
doc_id_to_update = "medis_b@mail.com"
update_payload = {
    "nomor_telepon": "081999998888",
    "profesi": "Dokter Spesialis Anak",
    # TIDAK PERLU _id atau _rev di sini, akan diambil oleh fungsi update_doc
}
update_doc("users", doc_id_to_update, update_payload)

# Verifikasi hasil update
print("\n--- VERIFIKASI UPDATE ---")
updated_doc = get_doc("users", doc_id_to_update)
if updated_doc:
    print(f"Nomor Telepon baru: {updated_doc['nomor_telepon']}")
    print(f"Profesi baru: {updated_doc['profesi']}")


## 4. DELETE (Menghapus)
print("\n--- DELETE: Hapus Dokumen Log Aktivitas 'log_001' ---")
doc_id_to_delete = "log_001"
delete_doc("log_aktivitas", doc_id_to_delete)

# Verifikasi hasil delete
print("\n--- VERIFIKASI DELETE ---")
if get_doc("log_aktivitas", doc_id_to_delete) is None:
    print(f"✅ Dokumen {doc_id_to_delete} berhasil dihapus (atau memang tidak ada).")
else:
    print(f"❌ Dokumen {doc_id_to_delete} GAGAL dihapus.")

print("\n" + "="*50)
print("              CRUD DEMO SELESAI")
print("="*50)