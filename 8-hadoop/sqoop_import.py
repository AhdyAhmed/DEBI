import subprocess

SQOOP_CONTAINER = "sqoop"

TABLES = [
    "users",
    "products",
    "orders",
    "order_items",
    "payments"
]

def exec_in_sqoop(cmd):
    subprocess.run(
        ["docker", "exec", SQOOP_CONTAINER, "bash", "-c", cmd],
        check=True
    )

def prepare_hdfs():
    exec_in_sqoop("hdfs dfs -mkdir -p /e-commerce/app_db")
    exec_in_sqoop("hdfs dfs -chmod -R 777 /e-commerce")

def sqoop_import(table):
    exec_in_sqoop(f"""
        export HADOOP_CLASSPATH=/usr/share/java/mysql-connector-java.jar &&
        export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:. &&
        sqoop import \
          -Dmapreduce.framework.name=local \
          --connect jdbc:mysql://mysql:3306/app_db \
          --username root \
          --password password \
          --table {table} \
          --target-dir /e-commerce/app_db/{table} \
          --delete-target-dir \
          --bindir . \
          -m 1
    """)

if __name__ == "__main__":
    print("🧩 Preparing Hadoop environment...")
    prepare_hdfs()

    for table in TABLES:
        print(f"🚀 Importing {table}")
        sqoop_import(table)

    print("✅ Sqoop import completed successfully")
