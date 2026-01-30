import subprocess
import time

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
# Run command inside Docker container
# -----------------------------
def run_in_container(cmd):
    docker_cmd = f'docker exec -i {DOCKER_CONTAINER} bash -c "{cmd}"'
    run_command(docker_cmd)

# -----------------------------
# Setup preprocessing steps
# -----------------------------
def setup_environment():
    print("Starting environment setup...")
    
    # Step 1: Update /etc/hosts
    print("1. Updating /etc/hosts...")
    hosts_content = """172.18.0.3  namenode cluster-master
172.18.0.2  mysql"""
    
    hosts_cmd = f'echo "{hosts_content}" >> /etc/hosts'
    run_in_container(hosts_cmd)
    
    # Step 2: Download and install MySQL connector
    print("2. Downloading MySQL connector...")
    connector_cmds = """
    cd /tmp
    wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar
    cp mysql-connector-java-8.0.28.jar /usr/local/hive/lib/
    cp mysql-connector-java-8.0.28.jar /usr/local/sqoop/lib/
    cp mysql-connector-java-8.0.28.jar /usr/share/java/mysql-connector-java.jar
    """
    run_in_container(connector_cmds)
    
    # Step 3: Set HADOOP_CLASSPATH
    print("3. Setting HADOOP_CLASSPATH...")
    classpath_cmd = """
    export HADOOP_CLASSPATH=/usr/share/java/mysql-connector-java.jar
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:.
    echo 'export HADOOP_CLASSPATH=/usr/share/java/mysql-connector-java.jar:.' >> ~/.bashrc
    """
    run_in_container(classpath_cmd)
    
    # Step 4: Upgrade Hive schema
    print("4. Upgrading Hive schema...")
    schema_cmd = "schematool -dbType mysql -upgradeSchema"
    try:
        run_in_container(schema_cmd)
    except subprocess.CalledProcessError:
        print("Schema upgrade may have already been applied or encountered an error. Continuing...")
    
    # Step 5: Change to working directory
    print("5. Setting working directory...")
    run_in_container("mkdir -p /dataops && cd /dataops")
    
    print("Environment setup completed!\n")
    time.sleep(2)

# -----------------------------
# Export a single table
# -----------------------------
def export_table(table_name):
    hive_dir = f"/user/hive/warehouse/{MYSQL_DB}.db/{table_name}"
    
    sqoop_cmd = f"""
    export HADOOP_CLASSPATH=/usr/share/java/mysql-connector-java.jar:.
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
    --bindir /dataops \
    -m {MAPPER_COUNT}
    """
    
    docker_cmd = f'docker exec -i {DOCKER_CONTAINER} bash -c "{sqoop_cmd}"'
    
    print(f"Exporting table: {table_name}...")
    run_command(docker_cmd)
    print(f"Finished exporting table: {table_name}\n")

# -----------------------------
# Main process
# -----------------------------
def main():
    # Run setup first
    setup_environment()
    
    # Export all tables
    print("=" * 50)
    print("Starting table exports...")
    print("=" * 50 + "\n")
    
    for table in TABLES:
        export_table(table)
    
    print("=" * 50)
    print("All tables exported successfully!")
    print("=" * 50)

if __name__ == "__main__":
    main()