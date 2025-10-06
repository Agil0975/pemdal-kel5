from utils.CRUDAerospike import *
from utils.CRUDCouchDB import *

# --- SETUP: Create databases and seed data ---
print("\n" + "=" * 70)
print("              START COUCHDB CRUD DEMO & TESTS")
print("=" * 70)

# Create databases
db_list = [
    "users",
    "janji_temu",
    "pemesanan_obat",
    "pemesanan_layanan",
    "log_aktivitas",
]
for db in db_list:
    create_db(db)

# Seed Users
print("\n--- Seeding Users ---")
users_data = [
    {
        "_id": "pasien_a@mail.com",
        "kata_sandi": "hashed_pass_1",
        "nama_lengkap": "Ahmad Pasien",
        "tanggal_lahir": "1990-01-15",
        "nomor_telepon": "081211112222",
        "alamat": {
            "provinsi": "Jawa Barat",
            "kota": "Bandung",
            "jalan": "Jl. Merdeka No. 10",
        },
        "tipe_akun": "pasien",
        "status": "active",
    },
    {
        "_id": "medis_b@mail.com",
        "kata_sandi": "hashed_pass_2",
        "nama_lengkap": "Dr. Bunga Tenaga Medis",
        "tanggal_lahir": "1985-05-20",
        "nomor_telepon": "081333334444",
        "alamat": {
            "provinsi": "DKI Jakarta",
            "kota": "Jakarta Pusat",
            "jalan": "Jl. Sudirman No. 5",
        },
        "tipe_akun": "tenaga_medis",
        "NIKes": "1234567890",
        "profesi": "Dokter Umum",
        "id_rs": "RSU001",
        "nama_departemen": "Poli Umum",
        "status": "active",
    },
    {
        "_id": "pasien_c@mail.com",
        "kata_sandi": "hashed_pass_3",
        "nama_lengkap": "Citra Pasien",
        "tanggal_lahir": "1995-03-10",
        "nomor_telepon": "081555556666",
        "alamat": {
            "provinsi": "Jawa Barat",
            "kota": "Bandung",
            "jalan": "Jl. Asia Afrika No. 20",
        },
        "tipe_akun": "pasien",
        "status": "inactive",
    },
]
insert_docs("users", users_data)

# Seed Appointments
print("\n--- Seeding Janji Temu ---")
janji_temu_data = [
    {
        "_id": "jt_001",
        "waktu_pelaksanaan": datetime(2025, 11, 15, 10, 0, 0).isoformat(),
        "alasan": "Sakit kepala dan demam ringan",
        "email_pasien": "pasien_a@mail.com",
        "email_tenaga_medis": "medis_b@mail.com",
        "status": "terjadwal",
        "rs": {
            "nama_rs": "RS Utama Jaya",
            "email": "rsujaya@mail.com",
            "no_telepon": "02277778888",
            "provinsi": "Jawa Barat",
            "kota": "Bandung",
            "jalan": "Jl. Raya Timur",
        },
        "penyakit": "Flu",
        "detail_resep": [
            {
                "id_obat": "OBT001",
                "dosis": "3x sehari",
                "label_obat": "Paracetamol 500mg",
            }
        ],
    },
    {
        "_id": "jt_002",
        "waktu_pelaksanaan": datetime(2025, 8, 1, 14, 0, 0).isoformat(),
        "alasan": "Checkup rutin",
        "email_pasien": "pasien_c@mail.com",
        "email_tenaga_medis": "medis_b@mail.com",
        "status": "selesai",
    },
]
insert_docs("janji_temu", janji_temu_data)

# Seed Medicine Orders
print("\n--- Seeding Pemesanan Obat ---")
pemesanan_obat_data = [
    {
        "_id": "po_001",
        "email_pemesan": "pasien_a@mail.com",
        "waktu_pemesanan": datetime.now().isoformat(),
        "status_pemesanan": "Diproses",
        "detail_pesanan": [
            {
                "id_obat": "OBT001",
                "jumlah": 1,
                "harga_satuan": 50000,
                "label_obat": "Paracetamol 500mg",
            },
            {
                "id_obat": "OBT002",
                "jumlah": 2,
                "harga_satuan": 75000,
                "label_obat": "Vitamin C",
            },
        ],
    },
    {
        "_id": "po_002",
        "email_pemesan": "pasien_c@mail.com",
        "waktu_pemesanan": (datetime.now() - timedelta(days=60)).isoformat(),
        "status_pemesanan": "dibatalkan",
        "detail_pesanan": [
            {
                "id_obat": "OBT003",
                "jumlah": 1,
                "harga_satuan": 100000,
                "label_obat": "Antibiotik",
            }
        ],
    },
]
insert_docs("pemesanan_obat", pemesanan_obat_data)

# Seed Service Orders
print("\n--- Seeding Pemesanan Layanan ---")
pemesanan_layanan_data = [
    {
        "_id": "pl_001",
        "email_pemesan": "pasien_a@mail.com",
        "waktu_pemesanan": datetime.now().isoformat(),
        "jadwal_pelaksanaan": datetime(2025, 11, 16, 14, 0, 0).isoformat(),
        "status_pemesanan": "Terjadwal",
        "rs": {
            "nama_rs": "RS Utama Jaya",
            "email": "rsujaya@mail.com",
            "no_telepon": "02277778888",
            "provinsi": "Jawa Barat",
            "kota": "Bandung",
            "jalan": "Jl. Raya Timur",
        },
        "layanan": {"nama_layanan": "Fisioterapi", "biaya": 250000},
    }
]
insert_docs("pemesanan_layanan", pemesanan_layanan_data)

# Seed Activity Logs
print("\n--- Seeding Log Aktivitas ---")
log_aktivitas_data = [
    {
        "_id": "log_001",
        "id_perangkat": "BAYMIN_DEV_001",
        "waktu_aktivitas": (datetime.now() - timedelta(days=45)).isoformat(),
        "detail_aktivitas": "Pengukuran suhu tubuh, hasil: 37.0 C",
        "email_pasien": "pasien_a@mail.com",
    },
    {
        "_id": "log_002",
        "id_perangkat": "BAYMIN_DEV_002",
        "waktu_aktivitas": (datetime.now() - timedelta(days=5)).isoformat(),
        "detail_aktivitas": "Pengukuran tekanan darah, hasil: 120/80",
        "email_pasien": "pasien_a@mail.com",
    },
]
insert_docs("log_aktivitas", log_aktivitas_data)

# ========== CREATE INDEXES (ADD THIS SECTION) ==========
print("\n--- Creating Indexes for Better Query Performance ---")

# Index for sorting users by name
create_index("users", ["nama_lengkap"])

# Index for sorting appointments by date
create_index("janji_temu", ["waktu_pelaksanaan"])

# Index for querying users by status and city
create_index("users", ["status", "alamat.kota"])

# Index for querying users by type
create_index("users", ["tipe_akun"])

# Index for querying orders by status
create_index("pemesanan_obat", ["status_pemesanan"])

# Index for querying logs by date
create_index("log_aktivitas", ["waktu_aktivitas"])

print("✅ All indexes created!\n")

# ============================================================================
#                              TEST CASES
# ============================================================================

print("\n" + "=" * 70)
print("                         RUNNING TEST CASES")
print("=" * 70)

# --- TEST 1: query_docs - Simple Query ---
print("\n--- TEST 1: Query all patients ---")
query = {"selector": {"tipe_akun": "pasien"}}
patients = query_docs("users", query)
print(f"📊 Result: Found {len(patients)} patients")
for p in patients:
    print(f"   - {p['nama_lengkap']} ({p['_id']})")

# --- TEST 2: query_docs - Complex Query with Multiple Conditions ---
print("\n--- TEST 2: Query active patients in Bandung ---")
query = {
    "selector": {
        "$and": [
            {"tipe_akun": "pasien"},
            {"alamat.kota": "Bandung"},
            {"status": "active"},
        ]
    },
    "fields": ["nama_lengkap", "alamat.kota", "status"],
}
bandung_patients = query_docs("users", query)
print(f"📊 Result: Found {len(bandung_patients)} active patients in Bandung")
for p in bandung_patients:
    print(f"   - {p['nama_lengkap']} in {p['alamat']['kota']}")

# --- TEST 3: query_docs - Query with Sorting and Limit ---
print("\n--- TEST 3: Query users sorted by name, limit 2 ---")
query = {
    "selector": {"tipe_akun": {"$exists": True}},
    "sort": [{"nama_lengkap": "asc"}],
    "limit": 2,
}
limited_users = query_docs("users", query)
print(f"📊 Result: Retrieved {len(limited_users)} users (limit 2)")
for u in limited_users:
    print(f"   - {u['nama_lengkap']}")

# --- TEST 4: query_docs - Date Range Query ---
print("\n--- TEST 4: Query appointments after November 1, 2025 ---")
query = {
    "selector": {"waktu_pelaksanaan": {"$gt": "2025-11-01"}},
    "sort": [{"waktu_pelaksanaan": "desc"}],
}
future_appointments = query_docs("janji_temu", query)
print(f"📊 Result: Found {len(future_appointments)} future appointments")
for apt in future_appointments:
    print(f"   - {apt['_id']}: {apt['waktu_pelaksanaan']}")

# --- TEST 5: insert_docs - Single Document ---
print("\n--- TEST 5: Insert single new patient ---")
new_patient = {
    "_id": "pasien_d@mail.com",
    "nama_lengkap": "Dodi Pasien",
    "tipe_akun": "pasien",
    "alamat": {"provinsi": "Jawa Timur", "kota": "Surabaya"},
    "status": "active",
}
result = insert_docs("users", new_patient)
print(f"📊 Result: Insert {'succeeded' if result else 'failed'}")

# --- TEST 6: insert_docs - Bulk Insert ---
print("\n--- TEST 6: Bulk insert multiple logs ---")
new_logs = [
    {
        "_id": "log_003",
        "id_perangkat": "BAYMIN_DEV_003",
        "waktu_aktivitas": datetime.now().isoformat(),
        "detail_aktivitas": "Login sistem",
        "email_pasien": "pasien_d@mail.com",
    },
    {
        "_id": "log_004",
        "id_perangkat": "BAYMIN_DEV_004",
        "waktu_aktivitas": datetime.now().isoformat(),
        "detail_aktivitas": "Pengukuran gula darah",
        "email_pasien": "pasien_a@mail.com",
    },
]
results = insert_docs("log_aktivitas", new_logs)
print(f"📊 Result: Bulk insert completed, check output above")

# --- TEST 7: insert_docs - Duplicate ID (should fail) ---
print("\n--- TEST 7: Try to insert duplicate ID (should fail) ---")
duplicate = {"_id": "pasien_a@mail.com", "nama_lengkap": "This Should Fail"}
result = insert_docs("users", duplicate)
print(
    f"📊 Result: Insert {'failed as expected ✅' if not result else 'unexpectedly succeeded ❌'}"
)

# --- TEST 8: delete_docs_where - Delete by Status ---
print("\n--- TEST 8: Delete inactive users ---")
deleted_count = delete_docs_where("users", {"status": "inactive"})
print(f"📊 Result: Deleted {deleted_count} inactive users")

# --- TEST 9: delete_docs_where - Delete Old Logs ---
print("\n--- TEST 9: Delete logs older than 30 days ---")
thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
deleted_count = delete_docs_where(
    "log_aktivitas", {"waktu_aktivitas": {"$lt": thirty_days_ago}}
)
print(f"📊 Result: Deleted {deleted_count} old logs")

# --- TEST 10: delete_docs_where - Delete Cancelled Orders ---
print("\n--- TEST 10: Delete cancelled medicine orders ---")
deleted_count = delete_docs_where("pemesanan_obat", {"status_pemesanan": "dibatalkan"})
print(f"📊 Result: Deleted {deleted_count} cancelled orders")

# --- TEST 11: delete_docs_where - With Limit ---
print("\n--- TEST 11: Delete with limit (max 1 document) ---")
deleted_count = delete_docs_where("janji_temu", {"status": "selesai"}, limit=1)
print(f"📊 Result: Deleted {deleted_count} completed appointments (limit 1)")

# --- TEST 12: Verify Deletions ---
print("\n--- TEST 12: Verify deletions by querying ---")
remaining_inactive = query_docs("users", {"selector": {"status": "inactive"}})
remaining_cancelled = query_docs(
    "pemesanan_obat", {"selector": {"status_pemesanan": "dibatalkan"}}
)
print(f"📊 Result: {len(remaining_inactive)} inactive users remaining (should be 0)")
print(f"📊 Result: {len(remaining_cancelled)} cancelled orders remaining (should be 0)")

# --- TEST 13: Query After Deletions ---
print("\n--- TEST 13: Query all remaining users ---")
all_users = query_docs("users", {"selector": {"tipe_akun": {"$exists": True}}})
print(f"📊 Result: Total {len(all_users)} users in database")
for u in all_users:
    print(f"   - {u['nama_lengkap']} ({u.get('status', 'no status')})")

# --- TEST 14: Complex Query with Nested Fields ---
print("\n--- TEST 14: Query users in Jakarta Pusat ---")
jakarta_users = query_docs("users", {"selector": {"alamat.kota": "Jakarta Pusat"}})
print(f"📊 Result: Found {len(jakarta_users)} users in Jakarta Pusat")

# --- TEST 15: Query with $or Condition ---
print("\n--- TEST 15: Query patients OR doctors ---")
query = {"selector": {"$or": [{"tipe_akun": "pasien"}, {"profesi": {"$exists": True}}]}}
mixed_results = query_docs("users", query)
print(f"📊 Result: Found {len(mixed_results)} patients or doctors")

print("\n" + "=" * 70)
print("                    ALL TESTS COMPLETED!")
print("=" * 70)
