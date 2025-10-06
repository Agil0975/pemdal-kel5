import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.CRUDCouchDB import query_docs

"""
Read: Menampilkan rumah sakit dengan jumlah tenaga medis paling banyak.
"""

if __name__ == "__main__":

    start = time.time()

    # Step 1: Cari semua user tenaga medis
    query = {
        "selector": {"tipe_akun": "tenaga_medis"},
        "fields": ["email", "id_rs"],
        "limit": 10000,  # Add explicit limit
    }

    tenaga_medis_users = query_docs("user", query)

    # Step 2: Count tenaga medis per rs
    hospital_counts = {}

    for tm in tenaga_medis_users:
        id_rs = tm.get("id_rs")
        if id_rs:
            hospital_counts[id_rs] = hospital_counts.get(id_rs, 0) + 1

    # Step 3: Cari semua nama rs
    all_hospital_ids = list(hospital_counts.keys())

    hospitals_query = {
        "selector": {"id_rs": {"$in": all_hospital_ids}},  # Batch query
        "fields": ["id_rs", "nama"],
        "limit": len(all_hospital_ids) + 100,
    }
    all_hospitals = query_docs("rumah_sakit", hospitals_query)

    id_to_name = {h["id_rs"]: h["nama"] for h in all_hospitals}

    # Step 4: Format hasil
    results = [
        {
            "id_rs": id_rs,
            "nama_rumah_sakit": id_to_name.get(id_rs, "Unknown"),
            "jumlah_tenaga_medis": count,
        }
        for id_rs, count in hospital_counts.items()
    ]

    # Step 5: Sort dan ambil top 10
    results.sort(key=lambda x: x["jumlah_tenaga_medis"], reverse=True)
    top_10 = results[:10]

    end = time.time()

    # Hasil
    print("\nTop 10 Rumah Sakit by Medical Staff Count:")
    print("-" * 60)
    for i, hospital in enumerate(top_10, 1):
        print(f"{i}. {hospital['nama_rumah_sakit']} (ID: {hospital['id_rs']})")
        print(f"   Total Medical Staff: {hospital['jumlah_tenaga_medis']}")
        print()

    print(f"\nTime: {end - start:.2f}s")
