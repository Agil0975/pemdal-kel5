"""
INSERT 1: Menambahkan pengguna baru
Deskripsi: Menambahkan pengguna baru ke database CouchDB
"""
import sys
import os
import time
from faker import Faker

# Tambah path parent untuk import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.CRUDCouchDB import insert_docs, create_db

fake = Faker("id_ID")

def insert_new_user():
    """
    Menambahkan pengguna baru ke database user
    """
    print("INSERT 1: Adding new user")
    print("-" * 50)
    
    # Pastikan database user ada
    create_db("user")
    
    start_time = time.time()
    
    # Data pengguna baru sesuai struktur yang ditentukan
    new_user = {
        "email": fake.email(),
        "kata_sandi": "hashed_password_123",
        "nama_lengkap": fake.name(),
        "tanggal_lahir": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "nomor_telepon": fake.phone_number(),
        "alamat": {
            "provinsi": fake.state(),
            "kota": fake.city_name(),
            "jalan": fake.street_name()
        },
        "tipe_akun": "pasien"
    }
    
    # Insert ke database
    result = insert_docs("user", new_user)
    
    execution_time = (time.time() - start_time) * 1000
    
    if result:
        print(f"Query executed successfully")
        print(f"Email: {new_user['email']}")
        print(f"Name: {new_user['nama_lengkap']}")
        print(f"Location: {new_user['alamat']['kota']}, {new_user['alamat']['provinsi']}")
        print(f"Account Type: {new_user['tipe_akun']}")
        print(f"Execution time: {execution_time:.2f} ms")
        print(f"Rows affected: 1")
        
        return new_user
    else:
        print("Error: Failed to insert user")
        return None

def insert_multiple_users(count=5):
    """
    Menambahkan beberapa pengguna sekaligus (bulk insert)
    """
    print(f"\nBULK INSERT: Adding {count} users")
    print("-" * 50)
    
    start_time = time.time()
    
    users = []
    for i in range(count):
        user = {
            "email": fake.email(),
            "kata_sandi": "hashed_password_123",
            "nama_lengkap": fake.name(),
            "tanggal_lahir": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "nomor_telepon": fake.phone_number(),
            "alamat": {
                "provinsi": fake.state(),
                "kota": fake.city_name(),
                "jalan": fake.street_name()
            },
            "tipe_akun": "pasien"
        }
        users.append(user)
    
    # Bulk insert
    result = insert_docs("user", users)
    
    execution_time = (time.time() - start_time) * 1000
    
    if result:
        print(f"Bulk insert completed successfully")
        print(f"Execution time: {execution_time:.2f} ms")
        print(f"Rows affected: {count}")
        print(f"Throughput: {(count / execution_time * 1000):.2f} rows/sec")
        print(f"Average time per row: {execution_time/count:.2f} ms")
        
        return users
    else:
        print("Error: Bulk insert failed")
        return []

if __name__ == "__main__":
    print("Starting INSERT 1 - User Management")
    print("=" * 60)
    
    # Test 1: Insert single user
    user = insert_new_user()
    
    # Test 2: Bulk insert
    users = insert_multiple_users(5)
    
    print("\nINSERT 1 completed successfully")