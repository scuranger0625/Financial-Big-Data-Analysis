import os
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt

# 配置 Hadoop 和 Spark 環境變數
os.environ["HADOOP_HOME"] = r"C:\Users\Leon\Desktop\winutils-master\winutils-master\hadoop-3.3.6"
os.environ["HADOOP_COMMON_HOME"] = r"C:\Users\Leon\Desktop\winutils-master\winutils-master\hadoop-3.3.6"
os.environ["HADOOP_CONF_DIR"] = r"C:\Users\Leon\Desktop\winutils-master\winutils-master\hadoop-3.3.6\etc\hadoop"
os.environ["PATH"] += r";C:\Users\Leon\Desktop\winutils-master\winutils-master\hadoop-3.3.6\bin"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\tmp\spark_temp"
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Leon\AppData\Local\Programs\Python\Python310\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\Leon\AppData\Local\Programs\Python\Python310\python.exe'

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("KMeans Clustering with Visualization") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .getOrCreate()

# 數據集
data = [(0.0, 0.0), (1.0, 1.0), (9.0, 8.0), (8.0, 9.0)]
df = spark.createDataFrame(data, ["x", "y"])

# 特徵轉換
assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
dataset = assembler.transform(df)

# K-Means
kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(dataset)

# 預測與評估
predictions = model.transform(dataset)
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette with squared euclidean distance: {silhouette}")

# 可視化
clustered_data = predictions.select("x", "y", "prediction").collect()
x_coords = [row["x"] for row in clustered_data]
y_coords = [row["y"] for row in clustered_data]
clusters = [row["prediction"] for row in clustered_data]

centers = model.clusterCenters()
center_x = [center[0] for center in centers]
center_y = [center[1] for center in centers]

colors = ['red', 'blue']
cluster_colors = [colors[c] for c in clusters]

plt.figure(figsize=(8, 6))
plt.scatter(x_coords, y_coords, c=cluster_colors, s=100, label="Clustered Points")
plt.scatter(center_x, center_y, c='black', s=200, marker='X', label="Cluster Centers")
plt.title("K-Means Clustering Visualization")
plt.xlabel("X Coordinate")
plt.ylabel("Y Coordinate")
plt.legend()
plt.grid()
plt.show()
