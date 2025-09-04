import random
import time
import mysql.connector

# koneksi ke database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # sesuaikan kalau ada password root
    database="ecommerce_db"
)
cur = conn.cursor()

def insert_order():
    user_id = random.randint(1, 10)
    product_id = random.randint(1, 20)
    qty = random.randint(1, 5)
    price = random.randint(5000, 200000)  # harga per item

    # insert ke orders
    cur.execute("INSERT INTO orders (user_id, order_date, total) VALUES (%s, NOW(), %s)",
                (user_id, qty * price))
    order_id = cur.lastrowid

    # insert ke order_items
    cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s,%s,%s,%s)",
                (order_id, product_id, qty, price))

    # update stok di products
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

print("▶️ Generating NORMAL events... (Ctrl+C to stop)")
while True:
    action = random.choices(
        ["insert", "update_user", "delete"],
        weights=[0.7, 0.2, 0.1], k=1
    )[0]

    if action == "insert":
        insert_order()
    elif action == "update_user":
        update_user()
    elif action == "delete":
        delete_order()
    
    # jeda biar kayak real traffic
    time.sleep(random.uniform(0.5, 2.0))
