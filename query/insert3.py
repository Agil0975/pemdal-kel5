"""
INSERT 3: Menambah rumah sakit baru
Deskripsi: Menambahkan rumah sakit baru dengan departemen dan layanan medis
"""
import sys
import os
import time
import random
from faker import Faker

# Tambah path parent untuk import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.CRUDCouchDB import insert_docs, create_db, query_docs
from utils.CRUDAerospike import kv_insert

fake = Faker("id_ID")

# Enum layanan medis
LAYANAN_ENUM = [
    "vaksinasi",
    "fisioterapi", 
    "laboratorium",
    "radiologi",
    "konsultasi",
    "rehabilitasi",
    "bedah",
    "kardiologi",
    "neurologi",
    "pediatri"
]

DEPARTEMEN_ENUM = [
    "Umum",
    "Farmasi", 
    "Radiologi",
    "Laboratorium",
    "Bedah",
    "Kardiologi",
    "Neurologi",
    "Pediatri",
    "Kandungan",
    "Mata"
]

def generate_hospital_id():
    """
    Generate ID rumah sakit yang unik
    """
    # Ambil ID yang sudah ada
    existing_query = {
        "selector": {},
        "fields": ["id_rs"],
        "limit": 1000
    }
    
    existing_hospitals = query_docs("rumah_sakit", existing_query)
    existing_ids = [h.get("id_rs", "") for h in existing_hospitals]
    
    # Generate ID baru
    counter = 1
    while True:
        new_id = f"RS{counter:03d}"
        if new_id not in existing_ids:
            return new_id
        counter += 1

def create_new_hospital():
    """
    Membuat rumah sakit baru dengan departemen dan layanan lengkap
    """
    print("INSERT 3: Adding new hospital")
    print("-" * 50)
    
    # Pastikan database ada
    create_db("rumah_sakit")
    
    start_time = time.time()
    
    # Generate ID unik
    hospital_id = generate_hospital_id()
    
    # Pilih departemen secara random (3-5 departemen)
    selected_departments = random.sample(DEPARTEMEN_ENUM, random.randint(3, 5))
    departments = []
    for i, dept in enumerate(selected_departments):
        departments.append({
            "nama": dept,
            "gedung": chr(65 + i)  # A, B, C, dst
        })
    
    # Generate layanan medis sesuai struktur yang ditentukan
    layanan_medis = []
    for i, dept in enumerate(selected_departments):
        # Setiap departemen punya 2-3 layanan
        dept_services = random.sample(LAYANAN_ENUM, min(random.randint(2, 3), len(LAYANAN_ENUM)))
        
        for j, service in enumerate(dept_services):
            layanan_medis.append({
                "id_layanan": f"L{hospital_id[2:]}{i+1}{j+1}",
                "nama_layanan": service,
                "biaya": random.randint(100_000, 1_000_000)
            })
    
    # Data rumah sakit baru sesuai struktur yang ditentukan
    new_hospital = {
        "id_rs": hospital_id,
        "nama": f"RS {fake.company()}",
        "alamat": {
            "provinsi": fake.state(),
            "kota": fake.city_name(),
            "jalan": fake.street_name()
        },
        "departemen": departments,
        "layanan_medis": layanan_medis
    }
    
    # Insert ke CouchDB
    result = insert_docs("rumah_sakit", new_hospital)
    
    # Hospital data created successfully
    
    execution_time = (time.time() - start_time) * 1000
    
    if result:
        print(f"Hospital added successfully")
        print(f"Hospital ID: {new_hospital['id_rs']}")
        print(f"Hospital Name: {new_hospital['nama']}")
        print(f"Location: {new_hospital['alamat']['kota']}, {new_hospital['alamat']['provinsi']}")
        print(f"Departments: {len(departments)}")
        print(f"Services: {len(layanan_medis)}")
        print(f"Execution time: {execution_time:.2f} ms")
        print(f"Rows affected: 1")
        
        # Tampilkan detail departemen
        print(f"\nDepartments:")
        for dept in departments:
            print(f"   - {dept['nama']} (Building {dept['gedung']})")
        
        # Tampilkan sample layanan
        print(f"\nServices (showing {min(3, len(layanan_medis))}):") 
        for layanan in layanan_medis[:3]:
            print(f"   - {layanan['nama_layanan']} (ID: {layanan['id_layanan']}) - Rp {layanan['biaya']:,}")
        
        return new_hospital
    else:
        print("Error: Failed to add hospital")
        return None

def create_hospital_network(count=3):
    """
    Membuat jaringan rumah sakit (multiple hospitals)
    """
    print(f"\n=== BULK INSERT: Membuat Jaringan {count} Rumah Sakit ===")
    
    start_time = time.time()
    hospitals = []
    
    for i in range(count):
        print(f"\n🏥 Membuat rumah sakit {i+1}/{count}...")
        hospital = create_new_hospital()
        if hospital:
            hospitals.append(hospital)
        time.sleep(0.1)  # Small delay to avoid ID conflicts
    
    total_time = (time.time() - start_time) * 1000
    
    print(f"\n✅ Jaringan rumah sakit selesai dibuat!")
    print(f"🏥 Total: {len(hospitals)} rumah sakit")
    print(f"⏱️  Total waktu: {total_time:.2f} ms")
    print(f"📈 Rata-rata: {total_time/len(hospitals):.2f} ms/rumah sakit")
    
    return hospitals

if __name__ == "__main__":
    print("Starting INSERT 3 - Hospital Management")
    print("=" * 60)
    
    # Test 1: Insert single hospital
    hospital = create_new_hospital()
    
    print("\nINSERT 3 completed successfully")