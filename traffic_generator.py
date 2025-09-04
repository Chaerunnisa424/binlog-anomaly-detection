import random, time, mysql.connector
from datetime import datetime

# ====== Koneksi awal ke MySQL tanpa DB ======
root_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)
root_cur = root_conn.cursor()
root_cur.execute("CREATE DATABASE IF NOT EXISTS ecommerce_db")
root_conn.commit()
root_cur.close()
root_conn.close()

# ====== Koneksi ke DB ======
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="ecommerce_db"
)
cur = conn.cursor()

# ====== Buat tabel kalau belum ada ======
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    price INT,
    stock INT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    order_date DATETIME,
    total INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    price INT,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
)
""")

conn.commit()

# ====== SEED DATA ======
def seed_data():
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        print("👤 Insert users...")
        for i in range(1, 11):
            cur.execute("INSERT INTO users (username, email) VALUES (%s,%s)",
                        (f"user{i}", f"user{i}@mail.com"))

    cur.execute("SELECT COUNT(*) FROM products")
    if cur.fetchone()[0] == 0:
        print("📦 Insert products...")
        for i in range(1, 21):
            price = random.randint(10000, 200000)
            stock = random.randint(50, 200)
            cur.execute("INSERT INTO products (name, price, stock) VALUES (%s,%s,%s)",
                        (f"Product {i}", price, stock))

    conn.commit()
    print("✅ Seeding done")

# ====== NORMAL EVENTS ======
def insert_order():
    user_id = random.randint(1, 10)
    product_id = random.randint(1, 20)
    qty = random.randint(1, 5)
    price = random.randint(5000, 200000)
    cur.execute("INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
                (user_id, qty * price))
    order_id = cur.lastrowid
    cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
                (order_id, product_id, qty, price))
    cur.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (qty, product_id))
    conn.commit()

def update_user():
    user_id = random.randint(1, 10)
    new_email = f"user{user_id}_{random.randint(100,999)}@mail.com"
    cur.execute("UPDATE users SET email=%s WHERE id=%s", (new_email, user_id))
    conn.commit()

def delete_order():
    cur.execute("SELECT id FROM orders ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if row:
        order_id = row[0]
        cur.execute("DELETE FROM order_items WHERE order_id=%s", (order_id,))
        cur.execute("DELETE FROM orders WHERE id=%s", (order_id,))
        conn.commit()

# ====== ANOMALY EVENTS ======
def spike_inserts(n=1000):
    for _ in range(n):
        user_id = random.randint(1, 10)
        product_id = random.randint(1, 20)
        qty = random.randint(1, 5)
        price = random.randint(5000, 200000)
        cur.execute("INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
                    (user_id, qty * price))
        order_id = cur.lastrowid
        cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
                    (order_id, product_id, qty, price))
        cur.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (qty, product_id))
    conn.commit()
    print(f"🚨 Anomaly Spike {n} inserts at {datetime.now()}")

def ddl_event():
    cur.execute("SHOW COLUMNS FROM products LIKE 'dummy_col'")
    if not cur.fetchone():
        cur.execute("ALTER TABLE products ADD COLUMN dummy_col VARCHAR(50)")
        conn.commit()
        print(f"🚨 DDL event at {datetime.now()}")

def small_spike_update():
    for _ in range(50):
        user_id = random.randint(1, 10)
        product_id = random.randint(1, 20)
        qty = random.randint(1, 3)
        price = random.randint(5000, 100000)
        cur.execute("INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
                    (user_id, qty * price))
        order_id = cur.lastrowid
        cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
                    (order_id, product_id, qty, price))
        cur.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (qty, product_id))
    conn.commit()
    print(f"🚨 Small spike at {datetime.now()}")

# ====== MAIN LOOP ======
if __name__ == "__main__":
    seed_data()
    print("▶️ Starting traffic generator (24h)...")

    start_time = datetime.now()
    last_anomaly = None
    cooldown = 180  # minimal 3 menit antar anomaly

    while (datetime.now() - start_time).total_seconds() < 24*3600:
        now = datetime.now()
        
        # anomaly dengan cooldown
        if (last_anomaly is None or (now - last_anomaly).total_seconds() > cooldown) and random.random() < 0.06:
            choice = random.choice(["spike", "ddl", "small_spike"])
            if choice == "spike":
                spike_inserts(500)
            elif choice == "ddl":
                ddl_event()
            else:
                small_spike_update()
            last_anomaly = now
        else:
            # normal event
            action = random.choices(
                ["insert", "update_user", "delete"],
                weights=[0.7, 0.2, 0.1], k=1
            )[0]
            if action == "insert":
                insert_order()
            elif action == "update_user":
                update_user()
            else:
                delete_order()

        time.sleep(random.uniform(0.5, 2.0))

    print("⏹️ 24 hours ended, stopping traffic generator.")



# PS D:\Politeknik Harber Tegal\TEKNIK INFORMATIKA\SEMESTER 7\KPI\PT. Wabi Teknologi\Create_dataset> python traffic_generator.py
# 👤 Insert users...
# 📦 Insert products...
# ✅ Seeding done
# ▶️ Starting traffic generator (24h)...
# 🚨 Small spike at 2025-09-03 09:34:05.358250
# 🚨 Small spike at 2025-09-03 09:37:13.501655
# 🚨 Small spike at 2025-09-03 09:40:25.698997
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 09:43:34.592978
# 🚨 DDL event at 2025-09-03 09:47:20.162630
# 🚨 Small spike at 2025-09-03 09:51:47.389355
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 09:54:51.100300
# 🚨 Small spike at 2025-09-03 09:57:51.494452
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:01:35.163145
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:20:43.409669
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:27:16.682478
# 🚨 Small spike at 2025-09-03 10:33:38.858126
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:40:38.474134
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:44:06.211963
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:47:24.103130
# 🚨 Small spike at 2025-09-03 10:50:26.275444
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:54:55.907089
# 🚨 Small spike at 2025-09-03 10:50:26.275444
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:54:55.907089
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 10:54:55.907089
# 🚨 Small spike at 2025-09-03 10:57:58.457578
# 🚨 Small spike at 2025-09-03 10:57:58.457578
# 🚨 Small spike at 2025-09-03 11:04:11.084428
# 🚨 Small spike at 2025-09-03 11:04:11.084428
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:07:34.274887
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:07:34.274887
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:24:21.797500
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:24:21.797500
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:31:23.310591
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:31:23.310591
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:34:24.493819
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:34:24.493819
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:40:49.637944
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 11:40:49.637944
# 🚨 Small spike at 2025-09-03 11:43:50.182032
# 🚨 Small spike at 2025-09-03 11:43:50.182032
# 🚨 Small spike at 2025-09-03 11:50:29.995424
# 🚨 Small spike at 2025-09-03 11:50:29.995424
# 🚨 Small spike at 2025-09-03 11:56:56.361782
# 🚨 Small spike at 2025-09-03 12:16:33.006473
# 6.748251
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 13:24:15.841803
# 🚨 Small spike at 2025-09-03 13:27:43.201832
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 13:31:14.452143
# 🚨 Small spike at 2025-09-03 13:41:38.685447
# 🚨 Small spike at 2025-09-03 13:44:55.830132
# 🚨 Small spike at 2025-09-03 13:48:04.916157
# 🚨 Small spike at 2025-09-03 13:51:06.626457
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 14:02:51.386129
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 14:10:04.001530
# 🚨 Small spike at 2025-09-03 14:17:34.035454
# 🚨 Small spike at 2025-09-03 14:20:39.120694
# 🚨 Small spike at 2025-09-03 14:23:57.160854
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 14:27:20.136351
# 🚨 Small spike at 2025-09-03 14:30:23.298785
# 🚨 Small spike at 2025-09-03 14:36:56.318113
# 🚨 Anomaly Spike 500 inserts at 2025-09-03 14:44:18.882686
# Traceback (most recent call last):
#   File "D:\Politeknik Harber Tegal\TEKNIK INFORMATIKA\SEMESTER 7\KPI\PT. Wabi Teknologi\Create_dataset\traffic_generator.py", line 190, in <module>
#     time.sleep(random.uniform(0.5, 2.0))
# KeyboardInterrupt
# PS D:\Politeknik Harber Tegal\TEKNIK INFORMATIKA\SEMESTER 7\KPI\PT. Wabi Teknologi\Create_dataset> 