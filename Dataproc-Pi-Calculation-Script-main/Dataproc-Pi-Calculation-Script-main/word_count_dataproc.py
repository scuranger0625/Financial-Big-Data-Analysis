from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import time

# 設定參數
PROJECT_ID = "lively-wonder-439113-r9"  # 替換為您的專案 ID
REGION = "us-central1"  # 替換為您的區域
CLUSTER_NAME = "example-cluster"  # Dataproc 集羣名稱
BUCKET_NAME = "leonbigdataanalytics"  # 替換為您的 Cloud Storage 儲存桶名稱
INPUT_PATH = "gs://leonbigdataanalytics/input/"  # 替換為輸入檔案的 Cloud Storage 路徑
OUTPUT_PATH = "gs://leonbigdataanalytics/output/"  # 替換為輸出結果的路徑
SCRIPT_NAME = "word_count.py"  # 替換為 PySpark 程式名稱

# 上傳 Spark 腳本到 GCS
def upload_script(bucket_name, script_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(script_name)

    spark_code = """\
from pyspark import SparkContext
import sys

if len(sys.argv) != 3:
    raise Exception("Usage: word-count <input> <output>")

sc = SparkContext()
lines = sc.textFile(sys.argv[1])
counts = lines.flatMap(lambda line: line.split()) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(sys.argv[2])
"""
    blob.upload_from_string(spark_code)
    print(f"Uploaded {script_name} to bucket {bucket_name}.")

# 確保集羣存在

def ensure_cluster_exists(project_id, region, cluster_name):
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    try:
        cluster_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        print(f"Cluster '{cluster_name}' already exists.")
    except Exception as e:
        if "Not found" in str(e):
            print(f"Cluster '{cluster_name}' not found. Creating cluster...")
            create_cluster(project_id, region, cluster_name)  # 呼叫集羣建立函數
        else:
            raise e

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

# 提交 Spark 作業到 Dataproc
def submit_job(project_id, region, cluster_name, bucket_name, script_name, input_path, output_path):
    job_client = dataproc.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket_name}/{script_name}",
            "args": [input_path, output_path],
        },
    }

    operation = job_client.submit_job_as_operation(request={"project_id": project_id, "region": region, "job": job})
    print("Submitting Spark job...")
    response = operation.result()
    print(f"Job finished: {response.driver_output_resource_uri}")

# 讀取結果
def read_output(bucket_name, output_path):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=output_path.replace(f"gs://{bucket_name}/", ""))
    print("Word count results:")
    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        data = blob.download_as_text()
        print(data)

if __name__ == "__main__":
    # 步驟 1: 上傳 PySpark 腳本到 GCS
    upload_script(BUCKET_NAME, SCRIPT_NAME)

    # 步驟 2: 確保集羣存在
    ensure_cluster_exists(PROJECT_ID, REGION, CLUSTER_NAME)

    # 步驟 3: 提交 Spark 作業到 Dataproc
    submit_job(PROJECT_ID, REGION, CLUSTER_NAME, BUCKET_NAME, SCRIPT_NAME, INPUT_PATH, OUTPUT_PATH)

    # 步驟 4: 讀取輸出結果
    read_output(BUCKET_NAME, "output/")