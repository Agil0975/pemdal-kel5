import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.CRUDCouchDB import query_docs

"""
Read: Menampilkan pasien yang mengeluarkan biaya obat paling besar.
"""

if __name__ == "__main__":
    start = time.time()

    # Step 1: Cari pemesanan yang sudah selesai
    query = {"selector": {"status_pemesanan": "selesai"}, "limit": 10000}
    completed_orders = query_docs("pemesanan_obat", query)

    # Step 2: Hitung biaya total untuk setiap pasien
    patient_totals = {}

    for order in completed_orders:
        email = order["email_pemesan"]

        order_total = sum(
            item["harga_satuan"] * item["jumlah"]
            for item in order.get("detail_pesanan", [])
        )

        patient_totals[email] = patient_totals.get(email, 0) + order_total

    # Step 3: Cari semua nama user
    unique_emails = list(patient_totals.keys())

    user_query = {
        "selector": {"email": {"$in": unique_emails}},
        "fields": ["email", "nama_lengkap"],
        "limit": len(unique_emails) + 100,
    }
    all_users = query_docs("user", user_query)

    email_to_name = {user["email"]: user["nama_lengkap"] for user in all_users}

    # Step 4: Format hasil
    results = [
        {
            "email": email,
            "nama_lengkap": email_to_name.get(email, "Unknown"),
            "total_pengeluaran": total,
        }
        for email, total in patient_totals.items()
    ]

    # Step 5: Sort dan limit 5
    results.sort(key=lambda x: x["total_pengeluaran"], reverse=True)
    top_5 = results[:5]

    end = time.time()

    # Hasil
    print("Top 5 Patients by Total Spending:")
    print("-" * 60)
    for i, patient in enumerate(top_5, 1):
        print(f"{i}. {patient['nama_lengkap']} ({patient['email']})")
        print(f"   Total: Rp {patient['total_pengeluaran']:,}")
        print()

    print(f"\nTime: {end - start:.2f}s")
