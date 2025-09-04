import mysql.connector
import random

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",  # sesuaikan
    database="ecommerce_db"
)
cur = conn.cursor()

# Seed users
print("👤 Insert users...")
for i in range(1, 11):  # 10 user
    cur.execute("INSERT INTO users (username, email) VALUES (%s,%s)",
                (f"user{i}", f"user{i}@mail.com"))

# Seed products
print("📦 Insert products...")
for i in range(1, 21):  # 20 product
    price = random.randint(10000, 200000)
    stock = random.randint(50, 200)
    cur.execute("INSERT INTO products (name, price, stock) VALUES (%s,%s,%s)",
                (f"Product {i}", price, stock))

conn.commit()
print("✅ Seeding done")
