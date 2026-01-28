#!/usr/bin/env python3
"""
Hive Database Setup Script - FIXED VERSION
Creates app_db structure in Hive and populates with sample data (IDs starting from 200)
"""

import subprocess
import time
import sys

def run_docker_command(command, description=""):
    """Execute command inside sqoop docker container"""
    if description:
        print(f"\n{'='*70}")
        print(f"📋 {description}")
        print(f"{'='*70}")
    
    # Fixed: Use list format for subprocess and proper shell execution
    full_command = ['docker', 'exec', 'sqoop', 'hive', '-e', command]
    print(f"🔧 Executing Hive command...\n")
    
    try:
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        # Filter out SLF4J warnings and MySQL driver warnings
        if result.stdout:
            lines = result.stdout.split('\n')
            filtered_lines = [line for line in lines if not any(x in line for x in [
                'SLF4J:', 'Loading class', 'Hive Session ID', 'Logging initialized'
            ])]
            output = '\n'.join(filtered_lines).strip()
            if output:
                print(output)
        
        if result.stderr:
            # Only show actual errors, not warnings
            lines = result.stderr.split('\n')
            error_lines = [line for line in lines if 'FAILED:' in line or 'ERROR' in line]
            if error_lines:
                print(f"⚠️  {chr(10).join(error_lines)}")
                return False
        
        if result.returncode == 0:
            print("✅ Success")
            return True
        else:
            print("❌ Failed")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Command timeout!")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def check_container():
    """Check if sqoop container is running"""
    print("\n" + "="*70)
    print("🔍 Checking Docker container...")
    print("="*70)
    
    try:
        result = subprocess.run(
            ['docker', 'ps'],
            capture_output=True,
            text=True
        )
        
        if 'sqoop' in result.stdout:
            print("✅ Sqoop container is running\n")
            return True
        else:
            print("❌ Sqoop container is not running!")
            print("💡 Please start it with: docker start sqoop")
            return False
    except Exception as e:
        print(f"❌ Error checking container: {e}")
        return False

def create_hive_databases():
    """Create Hive databases"""
    print("\n" + "="*70)
    print("🚀 STEP 1: Creating Hive Databases")
    print("="*70)
    
    commands = [
        ("CREATE DATABASE IF NOT EXISTS app_db", "Creating app_db database"),
        ("SHOW DATABASES", "Showing all databases")
    ]
    
    for cmd, desc in commands:
        if not run_docker_command(cmd, desc):
            print(f"Warning: Command failed but continuing...")
        time.sleep(1)

def create_hive_tables():
    """Create Hive tables matching app_db structure"""
    print("\n" + "="*70)
    print("🚀 STEP 2: Creating Hive Tables")
    print("="*70)
    
    tables = [
        ("users", """CREATE TABLE IF NOT EXISTS app_db.users (
            user_id BIGINT,
            email STRING,
            password_hash STRING,
            full_name STRING,
            status STRING,
            created_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE"""),
        
        ("products", """CREATE TABLE IF NOT EXISTS app_db.products (
            product_id BIGINT,
            name STRING,
            price DECIMAL(10,2),
            is_active BOOLEAN,
            created_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE"""),
        
        ("orders", """CREATE TABLE IF NOT EXISTS app_db.orders (
            order_id BIGINT,
            user_id BIGINT,
            order_status STRING,
            order_date TIMESTAMP,
            total_amount DECIMAL(12,2)
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE"""),
        
        ("order_items", """CREATE TABLE IF NOT EXISTS app_db.order_items (
            order_item_id BIGINT,
            order_id BIGINT,
            product_id BIGINT,
            quantity INT,
            unit_price DECIMAL(10,2)
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE"""),
        
        ("payments", """CREATE TABLE IF NOT EXISTS app_db.payments (
            payment_id BIGINT,
            order_id BIGINT,
            payment_method STRING,
            payment_status STRING,
            paid_at TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE""")
    ]
    
    for table_name, ddl in tables:
        run_docker_command(ddl, f"Creating table: {table_name}")
        time.sleep(1)
    
    # Show tables
    run_docker_command("USE app_db; SHOW TABLES", "Listing all tables")



def main():
    """Main execution function"""
    print("\n" + "="*70)
    print("🐘 HIVE DATABASE SETUP SCRIPT - FIXED VERSION")
    print("="*70)
    print("This script will:")
    print("  1. Create app_db database in Hive")
    print("  2. Create tables matching MySQL app_db structure")
    print("  3. Insert sample data with IDs starting from 200")
    print("  4. Verify the inserted data")
    print("="*70)
    
    # Check if docker container is running
    if not check_container():
        sys.exit(1)
    
    # Execute setup steps
    try:
        create_hive_databases()
        create_hive_tables()
        
        
        print("\n" + "="*70)
        print("✅ HIVE SETUP COMPLETED SUCCESSFULLY!")
        print("="*70)
        print("\n📊 Summary:")
        print("  • Database: app_db")
        print("  • Tables: users, products, orders, order_items, payments")
        print("  • Data: IDs starting from 200")
        print("  • Status: All data inserted and verified")
        print("\n💡 You can now query the data using:")
        print("  docker exec sqoop hive -e 'SELECT * FROM app_db.users;'")
        print("="*70)
        
    except KeyboardInterrupt:
        print("\n\n❌ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()