"""
INSERT 2: Tambah pasien baru berdasarkan nama lengkap dari user
Deskripsi: Membuat pasien baru dengan mencari user yang sudah ada berdasarkan nama lengkap
"""
import sys
import os
import time
from faker import Faker

# Tambah path parent untuk import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.CRUDCouchDB import query_docs, insert_docs, create_db
from utils.CRUDAerospike import kv_insert

fake = Faker("id_ID")

def find_user_by_name(nama_lengkap):
    """
    Mencari user berdasarkan nama lengkap
    """
    query = {
        "selector": {
            "nama_lengkap": {"$eq": nama_lengkap},
            "tipe_akun": {"$eq": "pasien"}
        },
        "limit": 1
    }
    
    users = query_docs("user", query)
    return users[0] if users else None

def create_appointment_from_user(user_data):
    """
    Membuat janji temu berdasarkan data user yang sudah ada
    Sesuai dengan struktur janji_temu yang ditentukan
    """
    print("INSERT 2: Creating appointment from existing user")
    print("-" * 50)
    
    # Pastikan database ada
    create_db("janji_temu")
    
    start_time = time.time()
    
    # Ambil sample rumah sakit dan tenaga medis
    rs_query = {"selector": {}, "limit": 1}
    hospitals = query_docs("rumah_sakit", rs_query)
    
    medis_query = {"selector": {"tipe_akun": "tenaga_medis"}, "limit": 1}
    tenaga_medis = query_docs("user", medis_query)
    
    if not hospitals or not tenaga_medis:
        print("Error: No hospitals or medical staff found")
        return None
    
    hospital = hospitals[0]
    dokter = tenaga_medis[0]
    
    # Buat janji temu sesuai struktur yang ditentukan
    janji_temu_id = f"JT{fake.random_int(1000, 9999)}"
    janji_temu = {
        "id_janji_temu": janji_temu_id,
        "waktu_pelaksanaan": fake.future_datetime(end_date="+30d").isoformat(),
        "alasan": fake.random_element(["checkup rutin", "konsultasi", "kontrol lanjutan"]),
        "status": "dijadwalkan",
        "email_pasien": user_data.get("email"),
        "email_tenaga_medis": dokter.get("email"),
        "rs": {
            "nama_rs": hospital.get("nama"),
            "email": f"info@{hospital.get('id_rs', 'hospital').lower()}.com",
            "no_telepon": fake.phone_number(),
            "provinsi": hospital.get("alamat", {}).get("provinsi", ""),
            "kota": hospital.get("alamat", {}).get("kota", ""),
            "jalan": hospital.get("alamat", {}).get("jalan", "")
        },
        "penyakit": fake.random_element(["flu", "hipertensi", "diabetes", "asma"]),
        "detail_resep": []
    }
    
    # Insert ke CouchDB
    result = insert_docs("janji_temu", janji_temu)
    
    # Insert ke Aerospike sesuai konvensi key-value
    user_key = f"user:{user_data['email']}:janji_temu"
    rs_key = f"rs:{hospital['id_rs']}:janji_temu"
    medis_key = f"tenaga_medis:{dokter['email']}:janji_temu"
    
    # Insert references ke Aerospike
    kv_insert(user_key, [janji_temu_id])
    kv_insert(rs_key, [janji_temu_id])
    kv_insert(medis_key, [janji_temu_id])
    
    execution_time = (time.time() - start_time) * 1000
    
    if result:
        print(f"Appointment created successfully")
        print(f"Appointment ID: {janji_temu_id}")
        print(f"Patient: {user_data['nama_lengkap']} ({user_data['email']})")
        print(f"Doctor: {dokter['nama_lengkap']} ({dokter['email']})")
        print(f"Hospital: {hospital['nama']}")
        print(f"Schedule: {janji_temu['waktu_pelaksanaan'][:19].replace('T', ' ')}")
        print(f"Reason: {janji_temu['alasan']}")
        print(f"Status: {janji_temu['status']}")
        print(f"Execution time: {execution_time:.2f} ms")
        print(f"Rows affected: 1 (CouchDB) + 3 (Aerospike references)")
        
        return janji_temu
    else:
        print("Error: Failed to create appointment")
        return None

def insert_appointment_from_existing_user():
    """
    Mencari user yang sudah ada dan buat janji temu
    """
    print("Finding existing patient users...")
    
    # Query untuk ambil sample user pasien
    query = {
        "selector": {
            "tipe_akun": {"$eq": "pasien"}
        },
        "limit": 5
    }
    
    users = query_docs("user", query)
    
    if users:
        print(f"Found {len(users)} patient users:")
        for i, user in enumerate(users):
            print(f"{i+1}. {user['nama_lengkap']} - {user['email']}")
        
        # Pilih user pertama untuk demo
        selected_user = users[0]
        print(f"\nCreating appointment for: {selected_user['nama_lengkap']}")
        
        return create_appointment_from_user(selected_user)
    else:
        print("Error: No patient users found")
        return None

def insert_appointment_by_name(nama_lengkap):
    """
    Mencari user berdasarkan nama lengkap dan buat janji temu
    """
    print(f"Searching user by name: {nama_lengkap}")
    
    user = find_user_by_name(nama_lengkap)
    
    if user:
        print(f"User found: {user['nama_lengkap']} - {user['email']}")
        return create_appointment_from_user(user)
    else:
        print(f"Error: User '{nama_lengkap}' not found")
        return None

if __name__ == "__main__":
    print("Starting INSERT 2 - Appointment Management")
    print("=" * 60)
    
    # Test 1: Buat janji temu dari user yang sudah ada
    appointment1 = insert_appointment_from_existing_user()
    
    # Test 2: Cari berdasarkan nama (jika ada)
    if appointment1:
        print(f"\nSearching for another user by name...")
        # Ambil nama dari database untuk test
        query = {
            "selector": {"tipe_akun": "pasien"},
            "limit": 1,
            "skip": 1
        }
        other_users = query_docs("user", query)
        if other_users:
            appointment2 = insert_appointment_by_name(other_users[0]['nama_lengkap'])
    
    print("\nINSERT 2 completed successfully")