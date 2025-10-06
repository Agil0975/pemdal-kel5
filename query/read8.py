import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.CRUDCouchDB import query_docs
from utils.CRUDAerospike import aero_client, NAMESPACE, SET_NAME
from utils.CRUDCouchDB import query_docs

"""
Read: Menampilkan 10 rumah sakit dengan jumlah janji temu terbanyak.
"""

if __name__ == "__main__":
    start = time.time()

    # Step 1: Cari id rs di Aerospike
    hospital_ids = set()

    scan = aero_client.scan(NAMESPACE, SET_NAME)

    def extract_hospital_ids(record):
        try:
            bins = record[2]
            stored_key = bins.get("key", "")

            if stored_key and isinstance(stored_key, str):
                if stored_key.startswith("rs:") and stored_key.endswith(":janji_temu"):
                    parts = stored_key.split(":")
                    if len(parts) == 3:
                        hospital_ids.add(parts[1])
        except Exception as e:
            pass

    scan.foreach(extract_hospital_ids)
    print(f"Found {len(hospital_ids)} hospitals")

    # Step 2: Untuk setiap rs, cari jumlah appointment dari Aerospike
    hospital_counts = {}

    for id_rs in hospital_ids:
        key = f"rs:{id_rs}:janji_temu"
        try:
            (_, _, record) = aero_client.get((NAMESPACE, SET_NAME, key))
            janji_list = record.get("value", [])
            hospital_counts[id_rs] = len(janji_list)
        except Exception as e:
            hospital_counts[id_rs] = 0

    # Step 3: Cari semua nama rs dari CouchDB
    all_hospital_ids = list(hospital_counts.keys())

    hospitals_query = {
        "selector": {"id_rs": {"$in": all_hospital_ids}},
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
            "total_janji": count,
        }
        for id_rs, count in hospital_counts.items()
    ]

    # Step 5: Sort dan ambil top 10
    results.sort(key=lambda x: x["total_janji"], reverse=True)
    top_10 = results[:10]

    end = time.time()

    # Hasil
    print("\nTop 10 Rumah Sakit by Appointments:")
    print("-" * 60)
    for i, hospital in enumerate(top_10, 1):
        print(f"{i}. {hospital['nama_rumah_sakit']} (ID: {hospital['id_rs']})")
        print(f"   Total Appointments: {hospital['total_janji']}")
        print()

    print(f"\nTime: {end - start:.2f}s")
