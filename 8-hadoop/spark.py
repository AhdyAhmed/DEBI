from pyspark.sql import SparkSession
from datetime import date

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
import os
from pyspark.sql import SparkSession

os.environ["SPARK_LOCAL_DIRS"] = "E:/spark-temp1"

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()


# --------------------------------------------------
# JDBC Config
# --------------------------------------------------
MYSQL_HOST = "mysql"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"

APP_DB = "app_db"
ANALYTICS_DB = "analytics_db"

MYSQL_JDBC_URL_APP = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{APP_DB}"
MYSQL_JDBC_URL_ANALYTICS = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{ANALYTICS_DB}"

JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

jdbc_props = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": JDBC_DRIVER
}

# --------------------------------------------------
# Load App DB Tables
# --------------------------------------------------
users_df = spark.read.jdbc(MYSQL_JDBC_URL_APP, "users", properties=jdbc_props)
orders_df = spark.read.jdbc(MYSQL_JDBC_URL_APP, "orders", properties=jdbc_props)
order_items_df = spark.read.jdbc(MYSQL_JDBC_URL_APP, "order_items", properties=jdbc_props)
products_df = spark.read.jdbc(MYSQL_JDBC_URL_APP, "products", properties=jdbc_props)
payments_df = spark.read.jdbc(MYSQL_JDBC_URL_APP, "payments", properties=jdbc_props)

# --------------------------------------------------
# Register Temp Views (SQL ONLY from now on)
# --------------------------------------------------
users_df.createOrReplaceTempView("users")
orders_df.createOrReplaceTempView("orders")
order_items_df.createOrReplaceTempView("order_items")
products_df.createOrReplaceTempView("products")
payments_df.createOrReplaceTempView("payments")

snapshot_date = date.today().isoformat()

# ==================================================
# 1️⃣ USER ORDER SUMMARY
# ==================================================
user_order_summary_sql = f"""
SELECT
    u.user_id,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    MAX(o.order_date) AS last_order_at,
    DATE('{snapshot_date}') AS snapshot_date
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id
"""

user_order_summary_df = spark.sql(user_order_summary_sql)

user_order_summary_df.write.jdbc(
    MYSQL_JDBC_URL_ANALYTICS,
    "user_order_summary",
    mode="overwrite",
    properties=jdbc_props
)

# ==================================================
# 2️⃣ DAILY SALES SUMMARY
# ==================================================
daily_sales_summary_sql = """
SELECT
    DATE(o.order_date) AS sales_date,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE p.payment_status = 'success'
GROUP BY DATE(o.order_date)
"""

daily_sales_summary_df = spark.sql(daily_sales_summary_sql)

daily_sales_summary_df.write.jdbc(
    MYSQL_JDBC_URL_ANALYTICS,
    "daily_sales_summary",
    mode="overwrite",
    properties=jdbc_props
)

# ==================================================
# 3️⃣ PRODUCT SALES RANKING
# ==================================================
product_sales_ranking_sql = f"""
SELECT
    oi.product_id,
    SUM(oi.quantity) AS total_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    DENSE_RANK() OVER (
        ORDER BY SUM(oi.quantity * oi.unit_price) DESC
    ) AS rank_position,
    DATE('{snapshot_date}') AS snapshot_date
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN payments p ON o.order_id = p.order_id
WHERE p.payment_status = 'success'
GROUP BY oi.product_id
"""

product_sales_ranking_df = spark.sql(product_sales_ranking_sql)

product_sales_ranking_df.write.jdbc(
    MYSQL_JDBC_URL_ANALYTICS,
    "product_sales_ranking",
    mode="overwrite",
    properties=jdbc_props
)

# ==================================================
# 4️⃣ COMPLEX METRIC (EXAMPLE)
# ==================================================
complex_query_sql = """
SELECT
    'paid_orders_ratio' AS result_key,
    COUNT(CASE WHEN p.payment_status = 'success' THEN 1 END) /
    COUNT(*) AS metric_value
FROM payments p
"""

complex_df = spark.sql(complex_query_sql) \
    .withColumn("query_name", spark.sql("SELECT 'payment_metrics' AS q").col("q"))

complex_df.write.jdbc(
    MYSQL_JDBC_URL_ANALYTICS,
    "complex_query_results",
    mode="append",
    properties=jdbc_props
)

spark.stop()
print("✅ Analytics pipeline completed successfully")
