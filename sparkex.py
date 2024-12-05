# 匯入必要的 PySpark 模組
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest

# 建立 SparkSession
spark = SparkSession.builder \
    .appName("ChiSquareTestExample") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# 定義資料 (每一行代表一組 [標籤, 特徵向量])
data = [
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
]

# 將資料轉換為 DataFrame 格式，包含 "label" 和 "features" 兩欄
df = spark.createDataFrame(data, ["label", "features"])

# 進行卡方檢定，測試 "features" 和 "label" 之間的關聯性
result = ChiSquareTest.test(df, "features", "label").head()

# 印出卡方檢定結果
print("pValues: " + str(result.pValues))  # 顯示 p-value
print("degreesOfFreedom: " + str(result.degreesOfFreedom))  # 顯示自由度
print("statistics: " + str(result.statistics))  # 顯示卡方統計量
