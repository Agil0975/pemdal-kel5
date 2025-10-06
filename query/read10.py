import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.CRUDCouchDB import query_docs


"""
Read: Menampilkan daftar pasien yang sudah pernah membuat janji temu tetapi tidak menghasilkan resep.
"""
if __name__ == "__main__":
    start = time.time()

    # Step 1: Cari janji temu tanpa detail_resep
    query = {
        "selector": {"detail_resep": {"$size": 0}},
        "fields": ["id_janji_temu", "email_pasien"],
        "limit": 10000,
    }

    appointments_without_results = query_docs("janji_temu", query)

    # Step 2: Cari semua patient emails unik
    unique_patients = set(
        appt.get("email_pasien")
        for appt in appointments_without_results
        if appt.get("email_pasien")
    )

    # Step 3: Cari semua nama pasien
    if unique_patients:
        user_query = {
            "selector": {"email": {"$in": list(unique_patients)}},
            "fields": ["nama_lengkap", "email"],
            "limit": len(unique_patients) + 100,
        }
        all_users = query_docs("user", user_query)

        results = [
            {"nama_lengkap": user["nama_lengkap"], "email": user["email"]}
            for user in all_users
        ]
    else:
        results = []

    results.sort(key=lambda x: x["nama_lengkap"])

    end = time.time()

    # Hasil
    print("\nPatients with Appointments Without Results:")
    print("-" * 60)
    for i, patient in enumerate(results, 1):
        print(f"{i}. {patient['nama_lengkap']} ({patient['email']})")

    print(f"\nTotal patients: {len(results)}")
    print(f"Time: {end - start:.2f}s")
