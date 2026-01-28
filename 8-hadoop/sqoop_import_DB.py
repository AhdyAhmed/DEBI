import subprocess

SQOOP_CONTAINER = "sqoop"

def exec_in_sqoop(cmd):
    subprocess.run(
        ["docker", "exec", SQOOP_CONTAINER, "bash", "-c", cmd],
        check=True
    )

def prepare_hdfs():
    # We only need the parent directory now
    exec_in_sqoop("hdfs dfs -mkdir -p /e-commerceDB2/app_db")
    exec_in_sqoop("hdfs dfs -chmod -R 777 /e-commerceDB2")

def sqoop_import_all():
    exec_in_sqoop(f"""
        export HADOOP_CLASSPATH=/usr/share/java/mysql-connector-java.jar &&
        export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:. &&
        sqoop import-all-tables \
          -Dmapreduce.framework.name=local \
          --connect jdbc:mysql://mysql:3306/app_db \
          --username root \
          --password password \
          --warehouse-dir /e-commerceDB2/app_db \
          --m 1
    """)

if __name__ == "__main__":
    print("🧩 Preparing Hadoop environment...")
    prepare_hdfs()

    print("🚀 Starting Full Database Import...")
    sqoop_import_all()

    print("✅ Full database import completed successfully")