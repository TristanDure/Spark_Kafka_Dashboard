"""
PySpark 大数据处理引擎
- 批处理：离线分析历史数据
- 流处理：实时处理Kafka消息
- SQL查询：Spark SQL（模拟Hive数据仓库）

技术组件覆盖：
  Spark Core / Spark SQL / Spark Streaming (3个组件)
  + Kafka (消息队列)
  + HDFS (分布式存储)
  = 共5个大数据组件
"""
import os
import sys
import json
import time
from collections import defaultdict

# PySpark 可能未安装，降级处理
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, sum as spark_sum, count, avg, max as spark_max,
        window, from_json, to_timestamp, lit
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class SparkProcessor:
    """PySpark大数据处理器"""

    def __init__(self, app_name="EcommerceAnalytics", master="local[1]"):
        self.app_name = app_name
        self.master = master
        self.spark = None
        if PYSPARK_AVAILABLE:
            self._init_spark()

    def _init_spark(self):
        """初始化Spark Session"""
        if not PYSPARK_AVAILABLE:
            return None
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    # ===== 批处理：离线数据分析 =====
    def batch_analysis(self, data_list):
        """
        离线批处理分析
        对应Hive数据仓库的ETL + OLAP统计
        """
        if not PYSPARK_AVAILABLE or self.spark is None:
            return self._batch_fallback(data_list)

        # 定义Schema
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("city", StringType()),
            StructField("gender", StringType()),
            StructField("age_group", StringType()),
            StructField("goods_type", StringType()),
            StructField("amount", DoubleType()),
            StructField("payment", StringType()),
            StructField("action", StringType()),
            StructField("device", StringType()),
        ])

        df = self.spark.createDataFrame(data_list, schema=schema)
        df.createOrReplaceTempView("transactions")

        # Spark SQL 查询（模拟Hive SQL）
        results = {}

        # 1. 各城市交易总额
        results["city_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT city, SUM(amount) as total_amount, COUNT(*) as count
                FROM transactions GROUP BY city ORDER BY total_amount DESC
            """)
        )

        # 2. 性别统计
        results["gender_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT gender, SUM(amount) as total_amount, COUNT(*) as count
                FROM transactions GROUP BY gender
            """)
        )

        # 3. 支付方式统计
        results["payment_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT payment, SUM(amount) as total_amount, COUNT(*) as count
                FROM transactions GROUP BY payment ORDER BY total_amount DESC
            """)
        )

        # 4. 消费类型统计
        results["goods_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT goods_type, SUM(amount) as total_amount, COUNT(*) as count
                FROM transactions GROUP BY goods_type ORDER BY total_amount DESC
            """)
        )

        # 5. 年龄段分析
        results["age_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT age_group, AVG(amount) as avg_amount, COUNT(*) as count
                FROM transactions GROUP BY age_group
            """)
        )

        # 6. 设备分析
        results["device_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT device, COUNT(*) as count
                FROM transactions GROUP BY device
            """)
        )

        # 7. 用户行为统计
        results["action_stats"] = self._df_to_dict(
            self.spark.sql("""
                SELECT action, COUNT(*) as count
                FROM transactions GROUP BY action
            """)
        )

        # 8. 总体指标
        row = self.spark.sql("""
            SELECT
                COUNT(*) as total_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT city) as active_cities
            FROM transactions
        """).first()
        results["overview"] = {
            "total_count": row["total_count"],
            "total_amount": round(row["total_amount"], 2),
            "avg_amount": round(row["avg_amount"], 2),
            "unique_users": row["unique_users"],
            "active_cities": row["active_cities"]
        }

        return results

    def _batch_fallback(self, data_list):
        """无PySpark时的本地降级处理"""
        results = defaultdict(lambda: defaultdict(float))

        for record in data_list:
            city = record.get("city", "未知")
            gender = record.get("gender", "未知")
            payment = record.get("payment", "未知")
            goods = record.get("goods_type", "未知")
            age = record.get("age_group", "未知")
            device = record.get("device", "未知")
            action = record.get("action", "未知")
            amount = float(record.get("amount", 0))

            results["city_stats"][city] += amount
            results["gender_stats"][gender] += amount
            results["payment_stats"][payment] += amount
            results["goods_stats"][goods] += amount
            results["age_stats"][age] += amount
            results["device_stats"][device] += 1
            results["action_stats"][action] += 1

        total_amount = sum(v for v in results["city_stats"].values())
        total_count = len(data_list)
        results["overview"] = {
            "total_count": total_count,
            "total_amount": round(total_amount, 2),
            "avg_amount": round(total_amount / total_count, 2) if total_count > 0 else 0,
            "unique_users": len(set(r.get("user_id") for r in data_list)),
            "active_cities": len(set(r.get("city") for r in data_list))
        }

        return {k: dict(v) for k, v in results.items()}

    def _df_to_dict(self, df):
        """DataFrame转字典列表"""
        return [row.asDict() for row in df.collect()]

    # ===== 实时流处理 =====
    def process_stream_batch(self, records):
        """处理一批实时数据并返回聚合结果"""
        # 按维度聚合
        city_amount = defaultdict(float)
        gender_amount = defaultdict(float)
        payment_amount = defaultdict(float)
        goods_amount = defaultdict(float)

        for r in records:
            amount = float(r.get("amount", 0))
            city_amount[r.get("city", "未知")] += amount
            gender_amount[r.get("gender", "未知")] += amount
            payment_amount[r.get("payment", "未知")] += amount
            goods_amount[r.get("goods_type", "未知")] += amount

        # 返回前端ECharts需要的格式
        cities = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "南京"]
        genders = ["男", "女"]
        payments = ["银联", "支付宝", "微信", "现金"]
        goods = ["交通", "餐饮", "娱乐", "教育", "住房", "其他"]

        result = []
        result.extend([city_amount.get(c, 0) for c in cities])
        result.extend([gender_amount.get(g, 0) for g in genders])
        result.extend([payment_amount.get(p, 0) for p in payments])
        result.extend([goods_amount.get(g, 0) for g in goods])
        return result

    # ===== 机器学习（扩展） =====
    def train_user_segmentation(self, data_list):
        """用户分群：K-Means聚类（Spark MLlib）"""
        if not PYSPARK_AVAILABLE or self.spark is None:
            return {"status": "PySpark not available"}

        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.evaluation import ClusteringEvaluator

        # 特征工程
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("action_num", DoubleType()),
        ])
        df = self.spark.createDataFrame(data_list, schema=schema)

        assembler = VectorAssembler(inputCols=["amount", "action_num"], outputCol="features")
        df = assembler.transform(df)

        kmeans = KMeans(k=3, seed=1)
        model = kmeans.fit(df)
        predictions = model.transform(df)

        return {
            "cluster_centers": [c.tolist() for c in model.clusterCenters()],
            "k": 3,
            "status": "trained"
        }

    def close(self):
        if self.spark:
            self.spark.stop()


# 全局单例
spark_processor = SparkProcessor()
