import random
import mysql.connector
from datetime import datetime, timedelta

# =========================
# Koneksi ke database
# =========================
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # sesuaikan
    database="ecommerce_db"
)
cur = conn.cursor()

# =========================
# Fungsi untuk mencatat event
# =========================
def log_event(step, event_type, description):
    timestamp = datetime.now()
    print(f"[Step {step}] {timestamp.strftime('%H:%M:%S')} | {event_type} | {description}")
    # Bisa ditambahkan insert ke tabel log jika mau:
    # cur.execute("INSERT INTO event_log (step, timestamp, event_type, description) VALUES (%s,%s,%s,%s)",
    #             (step, timestamp, event_type, description))
    # conn.commit()

# =========================
# Fungsi Spike Inserts
# =========================
def spike_inserts(n, step, description):
    for _ in range(n):
        user_id = random.randint(1, 10)
        product_id = random.randint(1, 20)
        qty = random.randint(1, 5)
        price = random.randint(5000, 200000)

        # Insert ke orders
        cur.execute(
            "INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
            (user_id, qty * price)
        )
        order_id = cur.lastrowid

        # Insert ke order_items
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, qty, price)
        )

        # Update stock produk
        cur.execute(
            "UPDATE products SET stock = stock - %s WHERE id = %s",
            (qty, product_id)
        )

    conn.commit()
    log_event(step, "SPIKE_INSERT", description)

# =========================
# Fungsi DDL Event Aman
# =========================
def ddl_event(step, description):
    # Cek apakah kolom sudah ada
    cur.execute("SHOW COLUMNS FROM products LIKE 'dummy_col'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE products ADD COLUMN dummy_col VARCHAR(50)")
        conn.commit()
        log_event(step, "DDL", description)
    else:
        log_event(step, "DDL_SKIPPED", description + " (kolom sudah ada)")

# =========================
# Fungsi Spike Kecil / Update Massal
# =========================
def small_spike_update(step, description):
    # Contoh spike kecil: 50 transaksi
    for _ in range(50):
        user_id = random.randint(1, 10)
        product_id = random.randint(1, 20)
        qty = random.randint(1, 3)
        price = random.randint(5000, 100000)

        cur.execute(
            "INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
            (user_id, qty * price)
        )
        order_id = cur.lastrowid
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
            (order_id, product_id, qty, price)
        )
        # Update stock massal beberapa produk
        cur.execute(
            "UPDATE products SET stock = stock - %s WHERE id = %s",
            (qty, product_id)
        )
    conn.commit()
    log_event(step, "SMALL_SPIKE_UPDATE", description)

# =========================
# Main Script
# =========================
if __name__ == "__main__":
    print("▶️ Generating ANOMALY events...")

    # Step 41 → Spike besar
    spike_inserts(n=1000, step=41, description="Spike INSERT ribuan transaksi")

    # Step 61 → DDL event
    ddl_event(step=61, description="ALTER TABLE products add dummy_col")

    # Step 76–77 → Anomaly kecil di tengah normal terakhir
    small_spike_update(step=76, description="Spike kecil / update massal di tengah periode normal")
    # Step 77 bisa diulang lagi jika mau:
    small_spike_update(step=77, description="Spike kecil / update massal lanjutan")

    print("✅ Anomaly simulation done")

    # Tutup koneksi
    cur.close()
    conn.close()




# import random
# import mysql.connector

# # =========================
# # Koneksi ke database
# # =========================
# conn = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="",  # sesuaikan
#     database="ecommerce_db"
# )
# cur = conn.cursor()

# # =========================
# # Fungsi Spike Inserts
# # =========================
# def spike_inserts(n=500):
#     for _ in range(n):
#         user_id = random.randint(1, 10)
#         product_id = random.randint(1, 20)
#         qty = random.randint(1, 5)
#         price = random.randint(5000, 200000)

#         # Insert ke orders
#         cur.execute(
#             "INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
#             (user_id, qty * price)
#         )
#         order_id = cur.lastrowid

#         # Insert ke order_items
#         cur.execute(
#             "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
#             (order_id, product_id, qty, price)
#         )

#         # Update stock produk
#         cur.execute(
#             "UPDATE products SET stock = stock - %s WHERE id = %s",
#             (qty, product_id)
#         )
#     conn.commit()
#     print(f"✅ Spike inserts selesai: {n} orders + order_items + update stock")

# # =========================
# # Fungsi DDL Event Aman
# # =========================
# def ddl_event():
#     # Cek apakah kolom sudah ada
#     cur.execute("SHOW COLUMNS FROM products LIKE 'dummy_col'")
#     if not cur.fetchone():
#         cur.execute("ALTER TABLE products ADD COLUMN dummy_col VARCHAR(50)")
#         conn.commit()
#         print("✅ Kolom 'dummy_col' ditambahkan")
#     else:
#         print("ℹ️ Kolom 'dummy_col' sudah ada, dilewati")

# # =========================
# # Main Script
# # =========================
# if __name__ == "__main__":
#     print("▶️ Generating ANOMALY events...")
    
#     print("Step 1: SPIKE INSERTS (ribuan transaksi dalam 1 menit)")
#     spike_inserts(n=1000)  # bisa ubah jumlah sesuai kebutuhan

#     print("Step 2: DDL event")
#     ddl_event()

#     print("✅ Anomaly simulation done")

#     # Tutup koneksi
#     cur.close()
#     conn.close()
