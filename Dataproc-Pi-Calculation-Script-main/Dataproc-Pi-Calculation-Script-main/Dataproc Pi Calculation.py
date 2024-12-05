from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import time

# 設定專案參數
PROJECT_ID = "lively-wonder-439113-r9"  # 替換為您的專案 ID
REGION = "us-central1"  # 替換為您的區域
CLUSTER_NAME = "example-cluster"
BUCKET_NAME = "leonbigdataanalytics"  # 替換為儲存桶名稱
JOB_FILE_NAME = "pi-calculation.py"

# 刪除 Dataproc Cluster
def delete_cluster(project_id, region, cluster_name):
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    try:
        operation = cluster_client.delete_cluster(request={"project_id": project_id, "region": region, "cluster_name": cluster_name})
        print("Deleting cluster...")
        operation.result()
        print("Cluster deleted.")
    except Exception as e:
        if "Not found" in str(e):
            print("Cluster does not exist. No need to delete.")
        else:
            print(f"Cluster deletion failed due to: {e}")

# 建立 Dataproc Cluster
def create_cluster(project_id, region, cluster_name):
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    cluster_config = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "gce_cluster_config": {
                "zone_uri": f"{region}-a",  # 指定區域
                "internal_ip_only": False,  # 如果不需要內部 IP，設置為 False
            },
            "software_config": {
                "image_version": "2.2-debian11",  # 最新支援的 Dataproc 映像版本
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 50,  # 主節點磁碟大小
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 50,  # 工作節點磁碟大小
                },
            },
        },
    }

    operation = cluster_client.create_cluster(request={"project_id": project_id, "region": region, "cluster": cluster_config})
    print("Creating cluster...")
    operation.result()
    print("Cluster created.")

# 上傳 Spark 任務到 GCS
def upload_job_to_gcs(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    spark_job_code = """\
from pyspark.sql import SparkSession
import random

spark = SparkSession.builder.appName('Pi Calculation').getOrCreate()

def calculate_pi(partitions):
    n = 100000 * partitions
    count = 0
    for _ in range(n):
        x, y = random.random(), random.random()
        if x**2 + y**2 <= 1:
            count += 1
    return 4 * count / n

partitions = 10
pi = calculate_pi(partitions)

# 將結果保存到 Cloud Storage
output_path = "gs://leonbigdataanalytics/pi-results"
spark.sparkContext.parallelize([f"Calculated value of Pi: {pi}"]).saveAsTextFile(output_path)
"""

    blob.upload_from_string(spark_job_code)
    print(f"Uploaded {file_name} to bucket {bucket_name}.")

# 提交 Spark 作業
def submit_spark_job(project_id, region, cluster_name, bucket_name, file_name):
    job_client = dataproc.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    job_details = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": f"gs://{bucket_name}/{file_name}"},
    }

    operation = job_client.submit_job_as_operation(request={"project_id": project_id, "region": region, "job": job_details})
    print("Submitting Spark job...")
    response = operation.result()
    print(f"Job finished: {response.driver_output_resource_uri}")

if __name__ == "__main__":
    delete_cluster(PROJECT_ID, REGION, CLUSTER_NAME)  # 確保刪除舊集群
    create_cluster(PROJECT_ID, REGION, CLUSTER_NAME)  # 創建新集群
    upload_job_to_gcs(BUCKET_NAME, JOB_FILE_NAME)  # 上傳 Spark 任務到 GCS
    submit_spark_job(PROJECT_ID, REGION, CLUSTER_NAME, BUCKET_NAME, JOB_FILE_NAME)  # 提交 Spark 作業
