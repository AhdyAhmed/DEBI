#!/usr/bin/env python3
"""
Hive Data Insertion Script
Inserts minimal sample data (2-3 records per table) with IDs starting from 200
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
    
    full_command = ['docker', 'exec', 'sqoop', 'hive', '-e', command]
    print(f"🔧 Executing INSERT command...\n")
    
    try:
        result = subprocess.run(
            full_command,
            capture_output=True,
            text=True,
            timeout=180  # 3 minutes for INSERT operations
        )
        
        # Filter out warnings
        if result.stdout:
            lines = result.stdout.split('\n')
            filtered_lines = [line for line in lines if not any(x in line for x in [
                'SLF4J:', 'Loading class', 'Hive Session ID', 'Logging initialized'
            ])]
            output = '\n'.join(filtered_lines).strip()
            if output:
                print(output)
        
        if result.stderr:
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
        print("❌ Command timeout after 3 minutes!")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def insert_users():
    """Insert 3 sample users"""
    print("\n" + "="*70)
    print("📝 Inserting Users (3 records)")
    print("="*70)
    
    insert_cmd = """INSERT INTO app_db.users VALUES
(200, 'user200@example.com', 'hash200', 'Alice Johnson', 'active', '2024-01-15 10:00:00'),
(201, 'user201@example.com', 'hash201', 'Bob Smith', 'active', '2024-01-16 11:30:00'),
(202, 'user202@example.com', 'hash202', 'Carol White', 'inactive', '2024-01-17 09:15:00') ;"""
    
    return run_docker_command(insert_cmd, "Inserting 3 users (IDs: 200-202)")

def insert_products():
    """Insert 3 sample products"""
    print("\n" + "="*70)
    print("📝 Inserting Products (3 records)")
    print("="*70)
    
    insert_cmd = """INSERT INTO app_db.products VALUES
(200, 'Laptop Pro 15', 1299.99, true, '2024-01-10 08:00:00'),
(201, 'Wireless Mouse', 29.99, true, '2024-01-10 08:30:00'),
(202, 'Mechanical Keyboard', 89.99, true, '2024-01-10 09:00:00');"""
    
    return run_docker_command(insert_cmd, "Inserting 3 products (IDs: 200-202)")

def insert_orders():
    """Insert 3 sample orders"""
    print("\n" + "="*70)
    print("📝 Inserting Orders (3 records)")
    print("="*70)
    
    insert_cmd = """INSERT INTO app_db.orders VALUES
(200, 200, 'paid', '2024-01-25 10:30:00', 1329.98),
(201, 201, 'shipped', '2024-01-25 11:00:00', 139.98),
(202, 202, 'pending', '2024-01-25 14:20:00', 89.99);"""
    
    return run_docker_command(insert_cmd, "Inserting 3 orders (IDs: 200-202)")

def insert_order_items():
    """Insert 3 sample order items"""
    print("\n" + "="*70)
    print("📝 Inserting Order Items (3 records)")
    print("="*70)
    
    insert_cmd = """INSERT INTO app_db.order_items VALUES
(200, 200, 200, 1, 1299.99),
(201, 200, 201, 1, 29.99),
(202, 201, 202, 1, 89.99);"""
    
    return run_docker_command(insert_cmd, "Inserting 3 order items (IDs: 200-202)")

def insert_payments():
    """Insert 2 sample payments"""
    print("\n" + "="*70)
    print("📝 Inserting Payments (2 records)")
    print("="*70)
    
    insert_cmd = """INSERT INTO app_db.payments VALUES
(200, 200, 'credit_card', 'success', '2024-01-25 10:32:00'),
(201, 201, 'paypal', 'success', '2024-01-25 11:05:00');"""
    
    return run_docker_command(insert_cmd, "Inserting 2 payments (IDs: 200-201)")



def main():
    """Main execution function"""
    print("\n" + "="*70)
    print("🐘 HIVE DATA INSERTION (Minimal Sample Data)")
    print("="*70)
    print("This script will insert minimal sample data:")
    print("  • 3 users (IDs: 200-202)")
    print("  • 3 products (IDs: 200-202)")
    print("  • 3 orders (IDs: 200-202)")
    print("  • 3 order items (IDs: 200-202)")
    print("  • 2 payments (IDs: 200-201)")
    print("="*70)
    
    # Check if container is running
    try:
        result = subprocess.run(
            ['docker', 'ps'],
            capture_output=True,
            text=True
        )
        
        if 'sqoop' not in result.stdout:
            print("❌ Sqoop container is not running!")
            print("💡 Please start it with: docker start sqoop")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error checking container: {e}")
        sys.exit(1)
    
    print("\n✅ Sqoop container is running")
    
    # Execute insertions one by one
    success_count = 0
    total_tables = 5
    
    try:
        if insert_users():
            success_count += 1
        time.sleep(2)
        
        if insert_products():
            success_count += 1
        time.sleep(2)
        
        if insert_orders():
            success_count += 1
        time.sleep(2)
        
        if insert_order_items():
            success_count += 1
        time.sleep(2)
        
        if insert_payments():
            success_count += 1
        time.sleep(2)
        
        # Verify data
        
        print("\n" + "="*70)
        if success_count == total_tables:
            print("✅ DATA INSERTION COMPLETED SUCCESSFULLY!")
        else:
            print(f"⚠️  PARTIAL SUCCESS: {success_count}/{total_tables} tables inserted")
        print("="*70)
        print("\n📊 Summary:")
        print(f"  • Successfully inserted: {success_count}/{total_tables} tables")
        print("  • Users: 3 records")
        print("  • Products: 3 records")
        print("  • Orders: 3 records")
        print("  • Order Items: 3 records")
        print("  • Payments: 2 records")
        print("\n💡 Query examples:")
        print("  docker exec sqoop hive -e 'SELECT * FROM app_db.users;'")
        print("  docker exec sqoop hive -e 'SELECT * FROM app_db.products;'")
        print("="*70)
        
    except KeyboardInterrupt:
        print("\n\n❌ Insertion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during insertion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()