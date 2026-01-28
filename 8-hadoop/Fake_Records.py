#pip install faker
import random
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def get_connection(database="app_db"):
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database=database
    )
def seed_users(n=50):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO users (email, password_hash, full_name, status)
    VALUES (%s, %s, %s, %s)
    """

    statuses = ["active", "inactive", "banned"]

    for _ in range(n):
        cursor.execute(sql, (
            fake.unique.email(),
            fake.sha256(),
            fake.name(),
            random.choice(statuses)
        ))

    conn.commit()
    cursor.close()
    conn.close()
def seed_products(n=30):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT INTO products (name, price, is_active)
    VALUES (%s, %s, %s)
    """

    for _ in range(n):
        cursor.execute(sql, (
            fake.word().capitalize(),
            round(random.uniform(5, 500), 2),
            random.choice([True, True, True, False])
        ))

    conn.commit()
    cursor.close()
    conn.close()

def seed_orders(n=100):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    sql = """
    INSERT INTO orders (user_id, order_status, order_date, total_amount)
    VALUES (%s, %s, %s, %s)
    """

    statuses = ["pending", "paid", "shipped", "cancelled"]

    for _ in range(n):
        order_date = fake.date_time_between(start_date="-90d", end_date="now")
        cursor.execute(sql, (
            random.choice(user_ids),
            random.choice(statuses),
            order_date,
            0  # will update after items
        ))

    conn.commit()
    cursor.close()
    conn.close()
def seed_order_items():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT order_id FROM orders")
    order_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id, price FROM products")
    products = cursor.fetchall()

    for order_id in order_ids:
        total = 0
        items_count = random.randint(1, 5)

        for _ in range(items_count):
            product_id, price = random.choice(products)
            qty = random.randint(1, 4)
            total += price * qty

            cursor.execute("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
            """, (order_id, product_id, qty, price))

        cursor.execute("""
            UPDATE orders
            SET total_amount = %s
            WHERE order_id = %s
        """, (round(total, 2), order_id))

    conn.commit()
    cursor.close()
    conn.close()
def seed_payments():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT order_id
        FROM orders
        WHERE order_status IN ('paid', 'shipped')
    """)
    order_ids = [row[0] for row in cursor.fetchall()]

    methods = ["card", "paypal", "bank"]

    for order_id in order_ids:
        cursor.execute("""
            INSERT INTO payments (order_id, payment_method, payment_status, paid_at)
            VALUES (%s, %s, 'success', %s)
        """, (
            order_id,
            random.choice(methods),
            fake.date_time_between(start_date="-90d", end_date="now")
        ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_users(50)
    seed_products(30)
    seed_orders(120)
    seed_order_items()
    seed_payments()

    print("✅ Random app data inserted successfully")
