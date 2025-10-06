import random
import string
import datetime
import requests
import aerospike
from faker import Faker

# ==========================
# KONFIGURASI DATABASE
# ==========================
COUCHDB_URL = "http://127.0.0.1:5984"
COUCHDB_AUTH = ("admin", "admin123")
AERO_CONFIG = {"hosts": [("127.0.0.1", 3000)]}

NAMESPACE = "test"
SET_NAME = "kv"

# ==========================
# ENUM DAN UTILITAS
# ==========================
OBAT_LABELS = ["analgesik", "antibiotik", "obat herbal"]
LAYANAN_ENUM = ["vaksinasi", "fisioterapi", "laboratorium", "radiologi", "konsultasi", "rehabilitasi"]
STATUS_ENUM = ["belum dibayar", "dijadwalkan", "sedang berlangsung", "selesai", "dibatalkan"]

fake = Faker("id_ID")

# ==========================
# HELPER: CouchDB
# ==========================
def create_db(db_name):
    res = requests.put(f"{COUCHDB_URL}/{db_name}", auth=COUCHDB_AUTH)
    if res.status_code in [201, 412]:
        print(f"Database {db_name} siap.")
    else:
        print(f"Gagal membuat database {db_name}: {res.text}")


def insert_docs(db_name, docs):
    url = f"{COUCHDB_URL}/{db_name}/_bulk_docs"
    res = requests.post(url, json={"docs": docs}, auth=COUCHDB_AUTH)
    if res.status_code in [201, 202]:
        print(f"{len(docs)} dokumen dimasukkan ke {db_name}.")
    else:
        print(f"Insert gagal ke {db_name}: {res.text}")

# ==========================
# HELPER: Aerospike
# ==========================
try:
    aero_client = aerospike.client(AERO_CONFIG).connect()
    print("Terhubung ke Aerospike.")
except Exception as e:
    print(f"Gagal konek ke Aerospike: {e}")
    aero_client = None


def kv_put(key, value):
    if aero_client:
        aero_client.put((NAMESPACE, SET_NAME, key), {"value": value})


def kv_append(key, value):
    if not aero_client:
        return
    try:
        (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
        existing = record.get("value", [])
    except aerospike.exception.RecordNotFound:
        existing = []
    existing.append(value)
    aero_client.put((NAMESPACE, SET_NAME, key), {"value": existing})


# ==========================
# SEEDER
# ==========================
def run_seeder():
    # 1. Siapkan DB untuk dokumen
    for db in ["user", "rumah_sakit", "obat", "pemesanan_layanan", "pemesanan_obat", "janji_temu"]:
        create_db(db)

    # 2. User (pasien + tenaga medis)
    users = []
    pasien_emails, tenaga_medis_emails = [], []

    for _ in range(5000):  # pasien
        email = fake.email()
        users.append({
            "email": email,
            "kata_sandi": "hashed123",
            "nama_lengkap": fake.name(),
            "tanggal_lahir": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "nomor_telepon": fake.phone_number(),
            "alamat": {
                "provinsi": fake.state(),
                "kota": fake.city_name(),
                "jalan": fake.street_name()
            },
            "tipe_akun": "pasien"
        })
        pasien_emails.append(email)

    for _ in range(5000):  # tenaga medis
        email = fake.email()
        id_rs = f"RS{random.randint(1,100):02d}"
        users.append({
            "email": email,
            "kata_sandi": "hashed123",
            "nama_lengkap": fake.name(),
            "tanggal_lahir": fake.date_of_birth().isoformat(),
            "nomor_telepon": fake.phone_number(),
            "alamat": {
                "provinsi": fake.state(),
                "kota": fake.city_name(),
                "jalan": fake.street_name()
            },
            "tipe_akun": "tenaga_medis",
            "NIKes": ''.join(random.choices(string.digits, k=8)),
            "profesi": random.choice(["dokter", "perawat"]),
            "id_rs": id_rs,
            "departemen": random.choice(["Radiologi", "Farmasi", "Umum"])
        })
        tenaga_medis_emails.append(email)

    insert_docs("user", users)

    # 3. Rumah sakit
    rumah_sakit_docs = []
    for i in range(100):
        id_rs = f"RS{i+1:02d}"
        rumah_sakit_docs.append({
            "id_rs": id_rs,
            "nama": f"RS {fake.company()}",
            "alamat": {
                "provinsi": fake.state(),
                "kota": fake.city_name(),
                "jalan": fake.street_name()
            },
            "departemen": [{"nama": d, "gedung": chr(65+i)} for d in ["Umum", "Farmasi", "Radiologi"]],
            "layanan_medis": [
                {"id_layanan": f"L{i+1}{j}", "nama_layanan": LAYANAN_ENUM[j], "biaya": random.randint(100_000, 500_000)}
                for j in range(3)
            ]
        })
    insert_docs("rumah_sakit", rumah_sakit_docs)

    # 4. Obat — SEKARANG DI COUCHDB
    obat_docs = []
    for i in range(500):
        id_obat = f"OB{i+1:03d}"
        obat_docs.append({
            "id_obat": id_obat,
            "nama": fake.word().capitalize(),
            "label": random.choice(OBAT_LABELS),
            "harga": random.randint(5000, 50000),
            "stok": random.randint(10, 200)
        })
    insert_docs("obat", obat_docs)
    print("Data obat dimasukkan ke CouchDB.")

    # 5. Baymin (tetap di Aerospike)
    baymin_list = []
    for i in range(1000):
        id_baymin = f"BM{i+1:03d}"
        warna = random.choice(["hitam", "putih", "merah", "biru"])
        email_pasien = random.choice(pasien_emails) if random.random() < 0.8 else None
        kv_put(f"baymin:{id_baymin}:warna", warna)
        if email_pasien:
            kv_put(f"baymin:{id_baymin}:email_pasien", email_pasien)
        baymin_list.append({"id": id_baymin, "email": email_pasien})
    print("Baymin dimasukkan ke Aerospike.")

    # 6. Log Aktivitas Baymin (key-value)
    for i in range(5000):
        perangkat = random.choice(baymin_list)
        waktu = datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 1000))
        detail = random.choice(["monitoring suhu tubuh", "pengiriman sinyal detak jantung", "cek koneksi internet"])
        kv_put(f"log_act:{perangkat['id']}:{waktu.isoformat()}", detail)
    print("Log aktivitas dimasukkan ke Aerospike.")

    # 7. Pemesanan Layanan
    layanan_docs = []
    for i in range(5000):
        email = random.choice(pasien_emails)
        rs = random.choice(rumah_sakit_docs)
        layanan = random.choice(rs["layanan_medis"])
        id_pesanan = f"PL{i+1:03d}"
        doc = {
            "id_pesanan": id_pesanan,
            "email_pemesan": email,
            "waktu_pemesanan": fake.date_time_this_year().isoformat(),
            "jadwal_pelaksanaan": fake.future_datetime(end_date='+30d').isoformat(),
            "status_pemesanan": random.choice(STATUS_ENUM),
            "rs": rs["alamat"] | {"nama_rs": rs["nama"]},
            "layanan": layanan
        }
        layanan_docs.append(doc)
        kv_append(f"user:{email}:pemesanan_layanan", id_pesanan)
    insert_docs("pemesanan_layanan", layanan_docs)

    # 8. Pemesanan Obat
    pemesanan_obat_docs = []
    for i in range(5000):
        email = random.choice(pasien_emails)
        id_pesanan = f"PO{i+1:03d}"
        detail_pesanan = []
        for _ in range(random.randint(1, 3)):
            obat = random.choice(obat_docs)
            detail_pesanan.append({
                "id_obat": obat["id_obat"],
                "nama_obat": obat["nama"],
                "jumlah": random.randint(1, 5),
                "harga_satuan": obat["harga"],
                "label_obat": obat["label"]
            })
        doc = {
            "id_pesanan": id_pesanan,
            "email_pemesan": email,
            "waktu_pemesanan": fake.date_time_this_year().isoformat(),
            "status_pemesanan": random.choice(STATUS_ENUM),
            "detail_pesanan": detail_pesanan
        }
        pemesanan_obat_docs.append(doc)
        kv_append(f"user:{email}:pemesanan_obat", id_pesanan)
    insert_docs("pemesanan_obat", pemesanan_obat_docs)

    # 9. Janji Temu
    janji_docs = []
    for i in range(5000):
        id_janji = f"JT{i+1:03d}"
        pasien = random.choice(pasien_emails)
        dokter = random.choice(tenaga_medis_emails)
        rs = random.choice(rumah_sakit_docs)
        obat = random.choice(obat_docs)
        detail_resep = [{
            "id_obat": obat["id_obat"],
            "nama_obat": obat["nama"],
            "dosis": f"{random.randint(1,3)}x sehari",
            "label_obat": obat["label"]
        }]
        doc = {
            "id_janji_temu": id_janji,
            "waktu_pelaksanaan": fake.future_datetime(end_date='+30d').isoformat(),
            "alasan": random.choice(["checkup", "kontrol lanjutan", "konsultasi umum"]),
            "email_pasien": pasien,
            "email_tenaga_medis": dokter,
            "rs": rs["alamat"] | {"nama_rs": rs["nama"]},
            "penyakit": random.choice(["flu", "asma", "hipertensi"]),
            "detail_resep": detail_resep
        }
        janji_docs.append(doc)
        kv_append(f"user:{pasien}:janji_temu", id_janji)
        kv_append(f"tenaga_medis:{dokter}:janji_temu", id_janji)
        kv_append(f"rs:{rs['id_rs']}:janji_temu", id_janji)
    insert_docs("janji_temu", janji_docs)

    print("\nSEEDER SELESAI — Semua data dummy berhasil dimasukkan ke CouchDB & Aerospike.")
    aero_client.close()


if __name__ == "__main__":
    run_seeder()
