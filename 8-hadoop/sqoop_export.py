import subprocess

# -----------------------------
# Configuration
# -----------------------------
DOCKER_CONTAINER = "sqoop"
MYSQL_HOST = "172.18.0.2"
MYSQL_PORT = "3306"
MYSQL_DB = "app_db"
MYSQL_USER = "root"
MYSQL_PASS = "password"
MAPPER_COUNT = 1  # Increase for large tables

TABLES = ["users", "products", "orders", "order_items", "payments"]

# -----------------------------
# Run a shell command
# -----------------------------
def run_command(cmd):
    subprocess.run(cmd, shell=True, check=True)

# -----------------------------
# Export a single table
# -----------------------------
def export_table(table_name):
    hive_dir = f"/user/hive/warehouse/{MYSQL_DB}.db/{table_name}"
    
    sqoop_cmd = f"""
    sqoop export \
    -Dmapreduce.framework.name=local \
    --connect jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} \
    --username {MYSQL_USER} \
    --password {MYSQL_PASS} \
    --table {table_name} \
    --export-dir {hive_dir} \
    --input-fields-terminated-by '\\001' \
    --input-null-string '\\\\N' \
    --input-null-non-string '\\\\N' \
    --bindir . \
    -m {MAPPER_COUNT}
    """
    
    docker_cmd = f'docker exec -i {DOCKER_CONTAINER} bash -c "{sqoop_cmd}"'
    
    print(f"Exporting table: {table_name}...")
    run_command(docker_cmd)
    print(f"Finished exporting table: {table_name}")

# -----------------------------
# Main process
# -----------------------------
def main():
    for table in TABLES:
        export_table(table)
    print("All tables exported successfully!")

if __name__ == "__main__":
    main()
