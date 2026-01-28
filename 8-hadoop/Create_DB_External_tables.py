import subprocess

SQOOP_CONTAINER = "sqoop"

def exec_in_sqoop(cmd):
    """Execute command in Sqoop container"""
    result = subprocess.run(
        ["docker", "exec", SQOOP_CONTAINER, "bash", "-c", cmd],
        check=True,
        capture_output=True,
        text=True
    )
    return result.stdout

def create_hive_database():
    """Create Hive database for e-commerce data"""
    hive_cmd = """
    hive -e "
    CREATE DATABASE IF NOT EXISTS ecommerce_db
    COMMENT 'E-commerce application database'
    LOCATION '/e-commerceDB2/app_db';
    "
    """
    print("📊 Creating Hive database...")
    exec_in_sqoop(hive_cmd)
    print("✅ Database 'ecommerce_db' created successfully")

def create_external_tables():
    """Create external tables in Hive for all imported tables"""
    
    tables_ddl = {
        "users": """
        CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.users (
            user_id BIGINT,
            email STRING,
            password_hash STRING,
            full_name STRING,
            status STRING,
            created_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/e-commerceDB2/app_db/users'
        TBLPROPERTIES ('skip.header.line.count'='0');
        """,
        
        "products": """
        CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.products (
            product_id BIGINT,
            name STRING,
            price DECIMAL(10,2),
            is_active BOOLEAN,
            created_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/e-commerceDB2/app_db/products'
        TBLPROPERTIES ('skip.header.line.count'='0');
        """,
        
        "orders": """
        CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.orders (
            order_id BIGINT,
            user_id BIGINT,
            order_status STRING,
            order_date TIMESTAMP,
            total_amount DECIMAL(12,2)
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/e-commerceDB2/app_db/orders'
        TBLPROPERTIES ('skip.header.line.count'='0');
        """,
        
        "order_items": """
        CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.order_items (
            order_item_id BIGINT,
            order_id BIGINT,
            product_id BIGINT,
            quantity INT,
            unit_price DECIMAL(10,2)
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/e-commerceDB2/app_db/order_items'
        TBLPROPERTIES ('skip.header.line.count'='0');
        """,
        
        "payments": """
        CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.payments (
            payment_id BIGINT,
            order_id BIGINT,
            payment_method STRING,
            payment_status STRING,
            paid_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/e-commerceDB2/app_db/payments'
        TBLPROPERTIES ('skip.header.line.count'='0');
        """
    }
    
    print("\n📋 Creating external tables...")
    for table_name, ddl in tables_ddl.items():
        print(f"   Creating table: {table_name}")
        hive_cmd = f'hive -e "{ddl}"'
        exec_in_sqoop(hive_cmd)
        print(f"   ✅ Table '{table_name}' created")

def verify_tables():
    """Verify tables were created and show sample data"""
    print("\n🔍 Verifying tables and showing sample data...\n")
    
    # Show all tables
    show_tables_cmd = 'hive -e "USE ecommerce_db; SHOW TABLES;"'
    print("Tables in ecommerce_db:")
    output = exec_in_sqoop(show_tables_cmd)
    print(output)
    
    



def main():
    print("="*60)
    print("🎯 HIVE DATABASE & EXTERNAL TABLES SETUP")
    print("="*60)
    
    try:
        # Step 1: Create database
        create_hive_database()
        
        # Step 2: Create external tables
        create_external_tables()
        
        # Step 3: Verify tables
        verify_tables()
        
        
        
        print("\n" + "="*60)
        print("✅ SETUP COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nYou can now query your data using Hive!")
        print("\nTo access Hive shell:")
        print("   docker exec -it sqoop bash")
        print("   hive")
        print("   USE ecommerce_db;")
        print("   SHOW TABLES;")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()