"""
INSERT 4: Menambah departemen tertentu pada rumah sakit
Deskripsi: Menambahkan departemen baru ke rumah sakit yang sudah ada
"""
import sys
import os
import time
import random
from faker import Faker

# Tambah path parent untuk import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.CRUDCouchDB import query_docs, update_doc, create_db

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
    "pediatri",
    "ortopedi",
    "dermatologi",
    "psikiatri",
    "gigi",
    "mata"
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
    "Mata",
    "Ortopedi",
    "Dermatologi",
    "Psikiatri",
    "Gigi",
    "THT",
    "Paru",
    "Ginjal",
    "Endokrin"
]

def get_available_hospitals():
    """
    Ambil daftar rumah sakit yang tersedia
    """
    query = {
        "selector": {},
        "fields": ["_id", "_rev", "id_rs", "nama", "departemen", "layanan_medis"],
        "limit": 10
    }
    
    hospitals = query_docs("rumah_sakit", query)
    return hospitals

def get_available_departments(hospital):
    """
    Dapatkan daftar departemen yang belum ada di rumah sakit
    """
    existing_departments = [dept["nama"] for dept in hospital.get("departemen", [])]
    available_departments = [dept for dept in DEPARTEMEN_ENUM if dept not in existing_departments]
    return available_departments

def generate_department_services(department_name, hospital_id):
    """
    Generate layanan medis untuk departemen baru
    """
    # Layanan yang cocok untuk departemen tertentu
    department_services = {
        "Kardiologi": ["konsultasi", "kardiologi", "rehabilitasi"],
        "Neurologi": ["konsultasi", "neurologi", "rehabilitasi"],
        "Pediatri": ["konsultasi", "pediatri", "vaksinasi"],
        "Ortopedi": ["konsultasi", "bedah", "fisioterapi"],
        "Dermatologi": ["konsultasi", "dermatologi"],
        "Psikiatri": ["konsultasi", "psikiatri", "rehabilitasi"],
        "Gigi": ["konsultasi", "gigi", "bedah"],
        "Mata": ["konsultasi", "mata", "bedah"],
        "THT": ["konsultasi", "bedah"],
        "Paru": ["konsultasi", "radiologi", "laboratorium"],
        "Ginjal": ["konsultasi", "laboratorium", "radiologi"],
        "Endokrin": ["konsultasi", "laboratorium"]
    }
    
    # Ambil layanan default atau gunakan yang umum
    possible_services = department_services.get(department_name, ["konsultasi", "laboratorium"])
    
    # Generate ID layanan baru
    services = []
    for i, service in enumerate(possible_services):
        services.append({
            "id_layanan": f"L{hospital_id[2:]}{department_name[:3].upper()}{i+1}",
            "nama_layanan": service,
            "biaya": random.randint(150_000, 800_000)
        })
    
    return services

def add_department_to_hospital(hospital_id, department_name):
    """
    Menambahkan departemen baru ke rumah sakit tertentu
    """
    print(f"INSERT 4: Adding department '{department_name}' to {hospital_id}")
    print("-" * 50)
    
    start_time = time.time()
    
    # Ambil data rumah sakit
    hospitals = query_docs("rumah_sakit", {
        "selector": {"id_rs": {"$eq": hospital_id}},
        "limit": 1
    })
    
    if not hospitals:
        print(f"Error: Hospital {hospital_id} not found")
        return False
    
    hospital = hospitals[0]
    
    # Cek apakah departemen sudah ada
    existing_departments = [dept["nama"] for dept in hospital.get("departemen", [])]
    if department_name in existing_departments:
        print(f"Warning: Department '{department_name}' already exists in {hospital['nama']}")
        return False
    
    # Buat departemen baru sesuai struktur yang ditentukan
    new_department = {
        "nama": department_name,
        "gedung": chr(65 + len(hospital.get("departemen", [])))  # A, B, C, dst
    }
    
    # Generate layanan medis untuk departemen baru
    new_services = generate_department_services(department_name, hospital_id)
    
    # Update data rumah sakit
    updated_departments = hospital.get("departemen", []) + [new_department]
    updated_services = hospital.get("layanan_medis", []) + new_services
    
    updates = {
        "departemen": updated_departments,
        "layanan_medis": updated_services,
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Update ke database
    result = update_doc("rumah_sakit", hospital["_id"], updates)
    
    execution_time = (time.time() - start_time) * 1000
    
    if result:
        print(f"Department added successfully")
        print(f"Hospital: {hospital['nama']}")
        print(f"Department: {department_name}")
        print(f"Building: {new_department['gedung']}")
        print(f"New services: {len(new_services)}")
        print(f"Total departments: {len(updated_departments)}")
        print(f"Total services: {len(updated_services)}")
        print(f"Execution time: {execution_time:.2f} ms")
        print(f"Rows affected: 1")
        
        # Tampilkan layanan baru
        print(f"\nNew Services:")
        for service in new_services:
            print(f"   - {service['nama_layanan']} (ID: {service['id_layanan']}) - Rp {service['biaya']:,}")
        
        return True
    else:
        print("Error: Failed to add department")
        return False

def interactive_add_department():
    """
    Mode interaktif untuk menambah departemen
    """
    print("SELECT HOSPITAL:")
    
    hospitals = get_available_hospitals()
    
    if not hospitals:
        print("Error: No hospitals found")
        return
    
    # Tampilkan daftar rumah sakit
    for i, hospital in enumerate(hospitals):
        dept_count = len(hospital.get("departemen", []))
        print(f"{i+1}. {hospital['nama']} (ID: {hospital['id_rs']}) - {dept_count} departments")
    
    # Pilih rumah sakit (auto-select pertama untuk demo)
    selected_hospital = hospitals[0]
    print(f"\nSelected: {selected_hospital['nama']} ({selected_hospital['id_rs']})")
    
    # Tampilkan departemen yang sudah ada
    existing_departments = [dept["nama"] for dept in selected_hospital.get("departemen", [])]
    print(f"Existing departments: {', '.join(existing_departments)}")
    
    # Tampilkan departemen yang bisa ditambah
    available_departments = get_available_departments(selected_hospital)
    
    if not available_departments:
        print("Warning: All departments already exist in this hospital")
        return
    
    print(f"\nAvailable departments to add:")
    for i, dept in enumerate(available_departments[:10]):  # Tampilkan 10 teratas
        print(f"{i+1}. {dept}")
    
    # Pilih departemen pertama untuk demo
    selected_department = available_departments[0]
    print(f"\nAdding department: {selected_department}")
    
    # Tambahkan departemen
    return add_department_to_hospital(selected_hospital['id_rs'], selected_department)

def bulk_add_departments():
    """
    Menambahkan multiple departemen ke berbagai rumah sakit
    """
    print("\n=== BULK ADD: Menambah Departemen ke Berbagai RS ===")
    
    hospitals = get_available_hospitals()[:3]  # Ambil 3 RS pertama
    success_count = 0
    
    for hospital in hospitals:
        available_departments = get_available_departments(hospital)
        
        if available_departments:
            # Tambah 1-2 departemen per RS
            departments_to_add = random.sample(available_departments, 
                                             min(2, len(available_departments)))
            
            for dept in departments_to_add:
                print(f"\n🔄 Menambah {dept} ke {hospital['nama']}...")
                if add_department_to_hospital(hospital['id_rs'], dept):
                    success_count += 1
                time.sleep(0.5)  # Delay untuk avoid conflicts
        else:
            print(f"⚠️  {hospital['nama']} sudah lengkap departemennya")
    
    print(f"\n✅ Bulk add selesai! {success_count} departemen berhasil ditambahkan.")

if __name__ == "__main__":
    print("Starting INSERT 4 - Department Management")
    print("=" * 60)
    
    # Test 1: Interactive add department
    success = interactive_add_department()
    
    print("\nINSERT 4 completed successfully")