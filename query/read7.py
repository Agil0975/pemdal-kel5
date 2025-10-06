import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.CRUDCouchDB import query_docs

"""
Read: Menampilkan layanan medis berdasarkan popularitas.
"""

if __name__ == "__main__":
    start = time.time()

    # Step 1: Ambil semua pemesanan_layanan
    all_orders = query_docs("pemesanan_layanan", {"selector": {}, "limit": 10000})

    # Step 2: Hitung jumlah layanan per nama_layanan
    service_counts = {}

    for order in all_orders:
        nama_layanan = order.get("layanan", {}).get("nama_layanan")

        if nama_layanan:
            service_counts[nama_layanan] = service_counts.get(nama_layanan, 0) + 1

    # Step 3: Ubah menjadi list dan di sort
    results = [
        {"nama_layanan": nama, "jumlah_pesanan": count}
        for nama, count in service_counts.items()
    ]

    results.sort(key=lambda x: x["jumlah_pesanan"], reverse=True)

    end = time.time()

    # Hasil
    print("Layanan Medis by Popularity:")
    print("-" * 50)
    for i, service in enumerate(results, 1):
        print(f"{i}. {service['nama_layanan']}: {service['jumlah_pesanan']} pesanan")

    print(f"\nTime: {end - start:.2f}s")
