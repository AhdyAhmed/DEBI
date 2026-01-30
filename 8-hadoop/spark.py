import os
os.environ['HADOOP_HOME'] = r'E:\hadoop-assignment\hadoop'
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# MySQL JDBC Driver configuration
MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
#MYSQL_JDBC_JAR = "mysql-connector-j-8.0.33.jar"  Make sure this jar is available

# MySQL connection properties
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"

def create_spark_session():
    """Create and configure Spark session with MySQL connector"""
    spark = SparkSession.builder \
        .appName("MySQL Analytics Pipeline") \
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.33") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_jdbc_url(database):
    """Generate JDBC URL for MySQL connection"""
    return f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{database}"

def read_table(spark, database, table):
    """Read table from MySQL using Spark JDBC"""
    jdbc_url = get_jdbc_url(database)
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .load()
    
    return df

def write_table(df, database, table, mode="overwrite"):
    """Write DataFrame to MySQL using Spark JDBC"""
    jdbc_url = get_jdbc_url(database)
    
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .mode(mode) \
        .save()

def run_user_order_summary(spark):
    """
    Calculate user order summary:
    - Total orders per user
    - Total amount spent
    - Last order date
    """
    print("Running: User Order Summary...")
    
    # Read necessary tables
    users = read_table(spark, "app_db", "users")
    orders = read_table(spark, "app_db", "orders")
    
    # Calculate summary
    user_summary = orders.filter(col("order_status") != "cancelled") \
        .groupBy("user_id") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_spent"),
            max("order_date").alias("last_order_at")
        ) \
        .withColumn("snapshot_date", lit(datetime.now().date()))
    
    # Write to analytics database
    write_table(user_summary, "analytics_db", "user_order_summary", mode="overwrite")
    
    print(f"✅ User Order Summary: {user_summary.count()} records written")
    return user_summary

def run_daily_sales_summary(spark):
    """
    Calculate daily sales summary:
    - Total orders per day
    - Total revenue per day
    - Average order value per day
    """
    print("Running: Daily Sales Summary...")
    
    orders = read_table(spark, "app_db", "orders")
    
    # Calculate daily summary
    daily_summary = orders.filter(col("order_status").isin(["paid", "shipped"])) \
        .withColumn("sales_date", to_date(col("order_date"))) \
        .groupBy("sales_date") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        ) \
        .orderBy("sales_date")
    
    # Write to analytics database
    write_table(daily_summary, "analytics_db", "daily_sales_summary", mode="overwrite")
    
    print(f"✅ Daily Sales Summary: {daily_summary.count()} records written")
    return daily_summary

def run_product_sales_ranking(spark):
    """
    Calculate product sales ranking:
    - Total quantity sold per product
    - Total revenue per product
    - Rank products by revenue
    """
    print("Running: Product Sales Ranking...")
    
    # Read necessary tables
    products = read_table(spark, "app_db", "products")
    order_items = read_table(spark, "app_db", "order_items")
    orders = read_table(spark, "app_db", "orders")
    
    # Join order_items with orders to filter only paid/shipped orders
    valid_orders = orders.filter(col("order_status").isin(["paid", "shipped"]))
    
    # Calculate product sales
    product_sales = order_items \
        .join(valid_orders, "order_id") \
        .groupBy("product_id") \
        .agg(
            sum("quantity").alias("total_sold"),
            sum(col("quantity") * col("unit_price")).alias("total_revenue")
        )
    
    # Add ranking
    from pyspark.sql.window import Window
    window_spec = Window.orderBy(col("total_revenue").desc())
    
    product_ranking = product_sales \
        .withColumn("rank_position", row_number().over(window_spec)) \
        .withColumn("snapshot_date", lit(datetime.now().date()))
    
    # Write to analytics database
    write_table(product_ranking, "analytics_db", "product_sales_ranking", mode="overwrite")
    
    print(f"✅ Product Sales Ranking: {product_ranking.count()} records written")
    return product_ranking

def run_complex_query_results(spark):
    """
    Run complex analytical queries and store results:
    - Customer lifetime value segments
    - Product category performance
    - Payment method distribution
    """
    print("Running: Complex Query Results...")
    
    # Read necessary tables
    users = read_table(spark, "app_db", "users")
    orders = read_table(spark, "app_db", "orders")
    payments = read_table(spark, "app_db", "payments")
    
    results = []
    
    # Query 1: Customer Lifetime Value Distribution
    clv_distribution = orders.filter(col("order_status") != "cancelled") \
        .groupBy("user_id") \
        .agg(sum("total_amount").alias("lifetime_value")) \
        .selectExpr(
            "'customer_ltv_avg' as query_name",
            "'average' as result_key",
            "avg(lifetime_value) as metric_value"
        )
    
    results.append(clv_distribution)
    
    # Query 2: Payment Success Rate
    payment_stats = payments \
        .groupBy("payment_status") \
        .count() \
        .withColumn("total", sum("count").over(Window.partitionBy())) \
        .withColumn("metric_value", (col("count") / col("total")) * 100) \
        .select(
            lit("payment_success_rate").alias("query_name"),
            col("payment_status").alias("result_key"),
            col("metric_value")
        )
    
    results.append(payment_stats)
    
    # Query 3: Average Order Value by Status
    avg_order_by_status = orders \
        .groupBy("order_status") \
        .agg(avg("total_amount").alias("metric_value")) \
        .select(
            lit("avg_order_value").alias("query_name"),
            col("order_status").alias("result_key"),
            col("metric_value")
        )
    
    results.append(avg_order_by_status)
    
    # Combine all results
    combined_results = results[0]
    for df in results[1:]:
        combined_results = combined_results.union(df)
    
    # Add timestamp and ID
    final_results = combined_results \
        .withColumn("generated_at", current_timestamp()) \
        .withColumn("result_id", monotonically_increasing_id())
    
    # Write to analytics database
    write_table(final_results, "analytics_db", "complex_query_results", mode="append")
    
    print(f"✅ Complex Query Results: {final_results.count()} records written")
    return final_results

def run_all_analytics(spark):
    """Run all analytics queries"""
    print("\n" + "="*60)
    print("Starting Analytics Pipeline")
    print("="*60 + "\n")
    
    start_time = datetime.now()
    
    # Run all analytics queries
    run_user_order_summary(spark)
    run_daily_sales_summary(spark)
    run_product_sales_ranking(spark)
    run_complex_query_results(spark)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print(f"Analytics Pipeline Completed in {duration:.2f} seconds")
    print("="*60 + "\n")

def main():
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all analytics
        run_all_analytics(spark)
        
        # Optional: Show sample results
        print("\nSample Results:")
        print("-" * 60)
        
        # Show top 10 users by spending
        user_summary = read_table(spark, "analytics_db", "user_order_summary")
        print("\nTop 10 Users by Total Spent:")
        user_summary.orderBy(col("total_spent").desc()).show(10)
        
        # Show recent daily sales
        daily_sales = read_table(spark, "analytics_db", "daily_sales_summary")
        print("\nRecent Daily Sales:")
        daily_sales.orderBy(col("sales_date").desc()).show(10)
        
        # Show top products
        product_ranking = read_table(spark, "analytics_db", "product_sales_ranking")
        print("\nTop 10 Products by Revenue:")
        product_ranking.orderBy("rank_position").show(10)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()