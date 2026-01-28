#pip install mysql-connector-python
import mysql.connector
from mysql.connector import errorcode

def get_connection(database=None):
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database=database
    )
def create_databases():
    conn = get_connection()
    cursor = conn.cursor()

    databases = ["app_db", "analytics_db"]

    for db in databases:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

    cursor.close()
    conn.close()
def create_app_tables():
    conn = get_connection("app_db")
    cursor = conn.cursor()

    tables = {}

    tables["users"] = """
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        full_name VARCHAR(255),
        status ENUM('active','inactive','banned') DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    tables["products"] = """
    CREATE TABLE IF NOT EXISTS products (
        product_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    tables["orders"] = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        user_id BIGINT NOT NULL,
        order_status ENUM('pending','paid','shipped','cancelled'),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(12,2),
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )
    """

    tables["order_items"] = """
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        order_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """

    tables["payments"] = """
    CREATE TABLE IF NOT EXISTS payments (
        payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        order_id BIGINT NOT NULL,
        payment_method VARCHAR(50),
        payment_status ENUM('success','failed','pending'),
        paid_at TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )
    """

    for name, ddl in tables.items():
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
def create_analytics_tables():
    conn = get_connection("analytics_db")
    cursor = conn.cursor()

    tables = {}

    tables["user_order_summary"] = """
    CREATE TABLE IF NOT EXISTS user_order_summary (
        user_id BIGINT PRIMARY KEY,
        total_orders INT,
        total_spent DECIMAL(14,2),
        last_order_at TIMESTAMP,
        snapshot_date DATE
    )
    """

    tables["daily_sales_summary"] = """
    CREATE TABLE IF NOT EXISTS daily_sales_summary (
        sales_date DATE PRIMARY KEY,
        total_orders INT,
        total_revenue DECIMAL(14,2),
        avg_order_value DECIMAL(10,2)
    )
    """

    tables["product_sales_ranking"] = """
    CREATE TABLE IF NOT EXISTS product_sales_ranking (
        product_id BIGINT,
        total_sold INT,
        total_revenue DECIMAL(14,2),
        rank_position INT,
        snapshot_date DATE,
        PRIMARY KEY (product_id, snapshot_date)
    )
    """

    tables["complex_query_results"] = """
    CREATE TABLE IF NOT EXISTS complex_query_results (
        result_id BIGINT PRIMARY KEY AUTO_INCREMENT,
        query_name VARCHAR(100),
        result_key VARCHAR(255),
        metric_value DECIMAL(18,4),
        generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    for name, ddl in tables.items():
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
if __name__ == "__main__":
    create_databases()
    create_app_tables()
    create_analytics_tables()
    print("✅ Databases and tables created successfully")
