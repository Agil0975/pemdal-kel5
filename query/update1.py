"""
UPDATE 1: Update expired order status
Description: Update order status from 'belum dibayar' to 'dibatalkan' for orders older than 2 days
"""
import sys
import os
import time
from datetime import datetime, timedelta

# Tambah path parent untuk import utils
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.CRUDCouchDB import query_docs, update_docs_where, create_db

def calculate_days_difference(order_date_str):
    """
    Hitung selisih hari antara tanggal pemesanan dengan sekarang
    """
    try:
        order_date = datetime.fromisoformat(order_date_str.replace('Z', '+00:00'))
        current_date = datetime.now()
        difference = current_date - order_date
        return difference.days
    except Exception as e:
        print(f"Error parsing date {order_date_str}: {e}")
        return 0

def find_overdue_orders():
    """
    Mencari pesanan obat yang belum dibayar dan sudah lewat 2 hari
    """
    print("UPDATE 1: Updating expired order status")
    print("-" * 50)
    print("Searching for orders older than 2 days...")
    
    start_time = time.time()
    
    # Query untuk mencari pesanan dengan status "belum dibayar"
    query = {
        "selector": {
            "status_pemesanan": {"$eq": "belum dibayar"}
        },
        "fields": ["_id", "_rev", "id_pesanan", "email_pemesan", "waktu_pemesanan", "status_pemesanan", "detail_pesanan"],
        "limit": 1000
    }
    
    unpaid_orders = query_docs("pemesanan_obat", query)
    query_time = (time.time() - start_time) * 1000
    
    print(f"Found {len(unpaid_orders)} orders with status 'belum dibayar'")
    print(f"Query time: {query_time:.2f} ms")
    
    if not unpaid_orders:
        print("No orders need to be updated")
        return [], 0
    
    # Filter pesanan yang sudah lewat 2 hari
    overdue_orders = []
    current_time = datetime.now()
    cutoff_date = current_time - timedelta(days=2)
    
    for order in unpaid_orders:
        order_date_str = order.get("waktu_pemesanan", "")
        if order_date_str:
            try:
                order_date = datetime.fromisoformat(order_date_str.replace('Z', '+00:00'))
                if order_date < cutoff_date:
                    days_overdue = (current_time - order_date).days
                    order["days_overdue"] = days_overdue
                    order["parsed_date"] = order_date
                    overdue_orders.append(order)
            except Exception as e:
                print(f"Warning: Error parsing date for {order['id_pesanan']}: {e}")
    
    print(f"Found {len(overdue_orders)} orders older than 2 days")
    
    # Sort berdasarkan tanggal tertua dan ambil hanya 5 pesanan pertama
    if overdue_orders:
        overdue_orders.sort(key=lambda x: x.get("parsed_date", datetime.now()))
        overdue_orders = overdue_orders[:5]
        print(f"Processing oldest 5 orders only")
    
    # Tampilkan 5 pesanan tertua yang akan diupdate
    if overdue_orders:
        print(f"\n5 oldest overdue orders to be updated:")
        for i, order in enumerate(overdue_orders):
            total_biaya = sum(item["harga_satuan"] * item["jumlah"] for item in order.get("detail_pesanan", []))
            print(f"   {i+1}. Order ID: {order['id_pesanan']} - User: {order['email_pemesan']}")
            print(f"      Date: {order['waktu_pemesanan'][:10]} ({order['days_overdue']} days ago)")
            print(f"      Total: Rp {total_biaya:,}")
    
    return overdue_orders, query_time

def update_overdue_orders(overdue_orders):
    """
    Update status pesanan yang overdue menjadi "dibatalkan"
    """
    if not overdue_orders:
        return 0, 0
    
    print(f"\nUpdating {len(overdue_orders)} overdue orders...")
    
    start_time = time.time()
    
    # Hitung statistik sebelum update
    total_value = 0
    order_ids = []
    
    for order in overdue_orders:
        order_value = sum(item["harga_satuan"] * item["jumlah"] for item in order.get("detail_pesanan", []))
        total_value += order_value
        order_ids.append(order["id_pesanan"])
    
    # Update menggunakan bulk update berdasarkan ID pesanan
    # Karena kita perlu update many documents, kita gunakan approach berbeda
    success_count = 0
    
    for order in overdue_orders:
        # Update individual order
        updates = {
            "status_pemesanan": "dibatalkan",
            "cancelled_reason": "Pembayaran tidak dilakukan dalam 2 hari",
            "cancelled_at": datetime.now().isoformat(),
            "cancelled_by": "system_auto",
            "previous_status": "belum dibayar"
        }
        
        # Menggunakan update_docs_where untuk setiap order
        updated_count = update_docs_where("pemesanan_obat", 
                                        {"id_pesanan": {"$eq": order["id_pesanan"]}}, 
                                        updates)
        
        if updated_count > 0:
            success_count += 1
    
    execution_time = (time.time() - start_time) * 1000
    
    print(f"Update completed")
    print(f"Successfully updated: {success_count}/{len(overdue_orders)} orders")
    print(f"Total cancelled value: Rp {total_value:,}")
    print(f"Update time: {execution_time:.2f} ms")
    print(f"Rows affected: {success_count}")
    
    return success_count, execution_time

def verify_updates():
    """
    Verifikasi hasil update dengan menghitung ulang
    """
    print(f"\nVERIFYING UPDATE RESULTS:")
    
    start_time = time.time()
    
    # Hitung pesanan yang masih "belum dibayar"
    still_unpaid_query = {
        "selector": {"status_pemesanan": {"$eq": "belum dibayar"}},
        "limit": 1000
    }
    still_unpaid = query_docs("pemesanan_obat", still_unpaid_query)
    
    # Hitung pesanan yang baru saja dibatalkan
    cancelled_today_query = {
        "selector": {
            "status_pemesanan": {"$eq": "dibatalkan"},
            "cancelled_by": {"$eq": "system_auto"}
        },
        "limit": 1000
    }
    cancelled_today = query_docs("pemesanan_obat", cancelled_today_query)
    
    verification_time = (time.time() - start_time) * 1000
    
    print(f"Orders still 'belum dibayar': {len(still_unpaid)}")
    print(f"Orders automatically cancelled: {len(cancelled_today)}")
    print(f"Verification time: {verification_time:.2f} ms")
    
    # Analisis pesanan yang masih belum dibayar
    if still_unpaid:
        overdue_count = 0
        current_time = datetime.now()
        cutoff_date = current_time - timedelta(days=2)
        
        for order in still_unpaid:
            order_date_str = order.get("waktu_pemesanan", "")
            if order_date_str:
                try:
                    order_date = datetime.fromisoformat(order_date_str.replace('Z', '+00:00'))
                    if order_date < cutoff_date:
                        overdue_count += 1
                except:
                    pass
        
        if overdue_count > 0:
            print(f"Warning: {overdue_count} overdue orders still not updated")
        else:
            print(f"All overdue orders successfully updated")
    
    return cancelled_today

def detailed_analysis(cancelled_orders):
    """
    Analisis detail pesanan yang dibatalkan
    """
    if not cancelled_orders:
        return
    
    print(f"\nDETAILED ANALYSIS:")
    
    # Analisis berdasarkan email pemesan
    email_stats = {}
    total_cancelled_value = 0
    
    for order in cancelled_orders:
        email = order.get("email_pemesan", "unknown")
        if email not in email_stats:
            email_stats[email] = {"count": 0, "total_value": 0}
        
        order_value = sum(item["harga_satuan"] * item["jumlah"] 
                         for item in order.get("detail_pesanan", []))
        
        email_stats[email]["count"] += 1
        email_stats[email]["total_value"] += order_value
        total_cancelled_value += order_value
    
    # Top customers dengan pesanan terbatalkan
    top_customers = sorted(email_stats.items(), 
                          key=lambda x: x[1]["total_value"], reverse=True)[:5]
    
    print(f"Total cancelled value: Rp {total_cancelled_value:,}")
    print(f"Affected customers: {len(email_stats)}")
    print(f"Average value per order: Rp {total_cancelled_value // len(cancelled_orders):,}")
    
    print(f"\nTop 5 customers with cancelled orders:")
    for i, (email, stats) in enumerate(top_customers):
        print(f"   {i+1}. {email}")
        print(f"      Orders: {stats['count']} orders")
        print(f"      Value: Rp {stats['total_value']:,}")

if __name__ == "__main__":
    print("Starting UPDATE 1 - Update Expired Order Status")
    print("=" * 60)
    
    total_start = time.time()
    
    # Step 1: Find overdue orders
    overdue_orders, query_time = find_overdue_orders()
    
    # Step 2: Update overdue orders
    if overdue_orders:
        success_count, update_time = update_overdue_orders(overdue_orders)
        
        # Step 3: Verify updates
        cancelled_orders = verify_updates()
        
        # Step 4: Detailed analysis
        detailed_analysis(cancelled_orders)
        
        total_time = (time.time() - total_start) * 1000
        
        print(f"\nOVERALL ANALYSIS:")
        print(f"Operation: UPDATE status (belum dibayar -> dibatalkan)")
        print(f"Database: CouchDB (pemesanan_obat)")
        print(f"Criteria: Orders > 2 days without payment")
        print(f"Total time: {total_time:.2f} ms")
        print(f"Query time: {query_time:.2f} ms")
        print(f"Update time: {update_time:.2f} ms")
        print(f"Success rate: {(success_count/len(overdue_orders)*100):.1f}%")
        print(f"Rows affected: {success_count}")
    
    else:
        print(f"\nRESULT:")
        print(f"No orders need to be updated")
        print(f"All orders within time limit or already paid")
        print(f"Rows affected: 0")
    
    print("\nUPDATE 1 completed successfully")