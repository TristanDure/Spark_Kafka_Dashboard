# 基于 Kafka + Spark + HDFS 的电商实时大数据分析平台

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-4.1.2-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.6.0-red.svg)](https://kafka.apache.org/)
[![Flask](https://img.shields.io/badge/Flask-2.x-lightgrey.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **课程**：大数据技术与应用 | **完成时间**：2026年6月  
> 模拟电商交易数据，通过 Kafka → Spark → HDFS 大数据组件实时处理，以 Web BI 大屏展示分析结果。

---

## 📖 目录

- [系统架构](#-系统架构)
- [技术栈](#-技术栈)
- [功能页面](#-功能页面)
- [快速开始](#-快速开始)
- [项目结构](#-项目结构)
- [运行模式](#-运行模式)
- [配置说明](#-配置说明)
- [组员协作指南](#-组员协作指南)
- [常见问题](#-常见问题)
- [参考资料](#-参考资料)

---

## 🏗 系统架构

```
┌──────────────────────────────────────────────────────────────┐
│                      数据可视化层                              │
│  BI仪表盘 │ 实时折线图 │ 柱状图对比 │ 地域分布 │ 消费分析      │
│              Flask + SocketIO + ECharts                       │
├──────────────────────────────────────────────────────────────┤
│                      数据处理层                                │
│   ┌──────────────────────────────────────────┐               │
│   │        Apache Spark (PySpark)             │               │
│   │  Spark Core │ Spark SQL │ Streaming │ MLlib│              │
│   └──────────────────────────────────────────┘               │
├──────────────────────────────────────────────────────────────┤
│                      消息队列层                                │
│   ┌──────────────────────────────────────────┐               │
│   │           Apache Kafka                    │               │
│   │  Producer (数据生成) → Topic → Consumer    │               │
│   └──────────────────────────────────────────┘               │
├──────────────────────────────────────────────────────────────┤
│                       存储层                                   │
│   ┌──────────────────────────────────────────┐               │
│   │         Apache Hadoop HDFS               │               │
│   │  /raw_data  │  /processed  │  /analytics  │               │
│   └──────────────────────────────────────────┘               │
├──────────────────────────────────────────────────────────────┤
│                      数据源层                                  │
│   数据生成器 → 50条/2秒 → 8城市、6消费类型、4支付方式           │
└──────────────────────────────────────────────────────────────┘
```

**数据流**：`数据生成器 → Kafka → PySpark(实时+离线) → HDFS → SocketIO → ECharts`

---

## 🔧 技术栈

| 层级 | 技术 | 版本 | 说明 |
|------|------|------|------|
| 消息队列 | **Apache Kafka** | 3.6.0 | 数据采集与传输，发布-订阅模式 |
| 计算引擎 | **PySpark** (Core/SQL/Streaming/MLlib) | 4.1.2 | 批处理 + 流处理 + SQL分析 |
| 分布式存储 | **HDFS** (兼容接口) | 3.3.6 | 三层目录：原始→处理→分析 |
| 数据仓库 | **Spark SQL** | 内置 | OLAP多维分析（替代Hive） |
| Web后端 | **Flask + SocketIO** | 2.x | REST API + 实时WebSocket推送 |
| 前端可视化 | **ECharts + jQuery** | 5.x | 7个交互式图表页面 |
| 容器化 | **Docker Compose** | — | Kafka + ZooKeeper 一键部署 |
| 构建工具 | **Maven** | — | Scala项目打包 |

---

## 🖥 功能页面

| 路由 | 页面 | 内容 |
|------|------|------|
| `/dashboard` | **BI仪表盘** | 6个KPI卡片 + 实时折线图 + 环形饼图 + 柱状图 |
| `/` | 实时总览 | 16维度实时趋势折线图 |
| `/city` | 城市对比 | 8城市交易额柱状图排名 |
| `/gender` | 性别分析 | 男女消费金额对比折线图 |
| `/payment` | 支付方式 | 银联/支付宝/微信/现金 对比 |
| `/goodstype` | 消费类型 | 交通/餐饮/娱乐/教育/住房/其他 |
| `/map` | 地域分布 | 城市散点图 + 排行柱状图 |

---

## 🚀 快速开始

### 环境要求

| 依赖 | 最低版本 | 说明 |
|------|----------|------|
| Python | 3.7+ | 推荐 Anaconda |
| Java | 8/11/23 | Spark 需要（23 需 `local[1]` 模式） |
| PySpark | 4.0+ | `pip install pyspark` |
| Kafka | 3.6.0 | 可选，不启动也能运行 |

### 1. 克隆仓库

```bash
git clone https://github.com/TristanDure/Spark_Kafka_Dashboard.git
cd Spark_Kafka_Dashboard
```

### 2. 安装 Python 依赖

```bash
pip install flask flask-socketio pyspark kafka-python
```

### 3. 启动系统

```bash
# 完整模式（含 PySpark 实时处理，推荐）
python run.py --spark

# 轻量 Demo（纯 Python，零外部依赖）
python python_dashboard/app_demo.py

# 全组件模式（需要 Kafka + PySpark）
python run.py --full
```

### 4. 打开浏览器

- BI仪表盘：http://localhost:5000/dashboard
- 实时折线图：http://localhost:5000/

### 5. （可选）启动 Kafka

```bash
# 使用 Docker Compose
cd docker
docker-compose up -d

# 验证 Kafka 运行
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 📁 项目结构

```
Spark_Kafka_Dashboard/
│
├── run.py                          # 一键启动脚本（支持多模式）
├── README.md                       # 项目说明（本文件）
├── 📋项目说明.md                    # 详细项目文档
├── 设计报告.md                      # 课程设计报告
├── 🎤答辩路演稿.md                  # 答辩演示稿
├── ⚠️注意事项.md                    # 避坑指南
├── 第三组结题报告PPT.pptx           # 答辩 PPT
│
├── python_dashboard/               # 【主项目】Flask 可视化应用
│   ├── app.py                      # 原始版应用
│   ├── app_demo.py                 # 轻量 Demo 版（零依赖）
│   ├── app_full.py                 # 完整版（Kafka + Spark + HDFS）
│   ├── setting.py                  # 全局配置文件
│   ├── scripts/
│   │   ├── data_generator.py       # 模拟数据生成器（50条/2秒）
│   │   ├── spark_processor.py      # PySpark 处理引擎（SQL + Streaming）
│   │   ├── hdfs_storage.py         # HDFS 存储层（含本地模拟）
│   │   ├── producer.py             # Kafka 生产者
│   │   ├── consumer.py             # Kafka 消费者
│   │   ├── dataprocessing.py       # 数据处理工具
│   │   └── kafka_pipeline.py       # Kafka 管道封装
│   ├── model/
│   │   └── consume_record_model.py # 数据模型
│   ├── util/
│   │   ├── DataUtil.py             # 数据工具类
│   │   └── JsonUtil.py             # JSON 序列化/反序列化
│   ├── templates/                  # HTML 模板（7个页面）
│   │   ├── dashboard.html          # BI 仪表盘大屏
│   │   ├── index.html              # 16维度实时折线图
│   │   ├── city.html               # 城市柱状图
│   │   ├── gender.html             # 性别对比
│   │   ├── payment.html            # 支付方式
│   │   ├── goods_type.html         # 消费类型
│   │   ├── map.html                # 地域分布
│   │   └── header.html             # 公共头部
│   ├── static/
│   │   ├── js/                     # ECharts、jQuery、SocketIO
│   │   └── styles/                 # CSS 样式
│   └── hdfs_storage/               # HDFS 本地模拟存储
│       ├── raw_data/               # 原始数据
│       └── analytics/              # 分析报表
│
├── DashboardDataProcessing/        # 【辅助项目】Scala Spark 流处理
│   ├── pom.xml                     # Maven 依赖配置
│   └── src/main/
│       ├── scala/KafkaProcessing.scala  # Scala版 Spark 处理
│       └── resources/log4j.properties
│
└── docker/
    └── docker-compose.yml          # Kafka + ZooKeeper 容器配置
```

---

## ⚙ 运行模式

系统支持**三级降级运行**，根据环境自动适配：

| 模式 | 命令 | 大数据组件 | 适用场景 |
|------|------|-----------|----------|
| 🟢 **Demo** | `python app_demo.py` | 0 个 | 快速看效果，无需任何配置 |
| 🟡 **PySpark** | `python run.py --spark` | 4 个 (Spark×3 + HDFS) | 答辩演示，完整数据处理 |
| 🔴 **全组件** | `python run.py --full` | 5 个 (+Kafka) | 生产环境，Docker 可运行时 |

```
Demo 模式：   数据生成 → 直接计算 → ECharts
PySpark 模式：数据生成 → PySpark(SQL+Streaming) → HDFS → ECharts
全组件模式：  数据生成 → Kafka → PySpark → HDFS → ECharts
```

---

## 🔩 配置说明

编辑 `python_dashboard/setting.py`：

```python
class BaseConfig(object):
    KAFKA_ADDRESS = 'localhost:9092'    # Kafka 地址
    GENERATE_INTERVAL = 2               # 数据生成间隔（秒）
    PRODUCER_TOPIC = 'test'             # 生产者 Topic
    CONSUMER_TOPIC = 'test_consumer'    # 消费者 Topic
    SPARK_HOST = 'local[*]'             # Spark 运行模式
    RECORD_COUNT = 50                   # 每批生成记录数
    GROUP_ID = 'ycliu'                  # Kafka 消费者组 ID
```

### Spark 模式说明

| 值 | 含义 |
|----|------|
| `local[*]` | 本地模式，使用所有 CPU 核心 |
| `local[1]` | 单 Worker（Windows + Java 23 兼容） |
| `spark://host:7077` | 连接到 Spark 集群 |

---

## 👥 组员协作指南

### 环境搭建（每个组员）

```bash
# 1. 克隆仓库
git clone https://github.com/TristanDure/Spark_Kafka_Dashboard.git
cd Spark_Kafka_Dashboard

# 2. 创建虚拟环境（推荐）
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

# 3. 安装依赖
pip install flask flask-socketio pyspark kafka-python

# 4. 验证运行
python run.py --spark
# 浏览器打开 http://localhost:5000/dashboard
```

### 协作流程

```bash
# 1. 拉取最新代码
git pull origin master

# 2. 创建你的功能分支
git checkout -b feature/你的功能名
# 例如: git checkout -b feature/add-ml-model
#       git checkout -b feature/fix-dashboard-style

# 3. 开发和提交
git add .
git commit -m "feat: 添加了XXX功能"

# 4. 推送分支
git push origin feature/你的功能名

# 5. 在 GitHub 上创建 Pull Request
# 访问 https://github.com/TristanDure/Spark_Kafka_Dashboard/pulls
# 通知组长 review 并合并
```

### 分支命名规范

| 前缀 | 用途 | 示例 |
|------|------|------|
| `feature/` | 新功能 | `feature/add-map-page` |
| `fix/` | 修复 Bug | `fix/socketio-timeout` |
| `docs/` | 文档更新 | `docs/update-readme` |
| `refactor/` | 代码重构 | `refactor/spark-module` |

### 分工建议

| 模块 | 涉及文件 | 建议人数 |
|------|----------|----------|
| 数据生成器 | `scripts/data_generator.py` | 1人 |
| Spark 处理引擎 | `scripts/spark_processor.py` | 1-2人 |
| HDFS 存储层 | `scripts/hdfs_storage.py` | 1人 |
| 前端可视化 | `templates/*.html`, `static/` | 1-2人 |
| Kafka/Docker | `docker/`, `scripts/producer.py` | 1人 |
| 文档 & PPT | `设计报告.md`, `🎤答辩路演稿.md` | 1人 |

---

## ❓ 常见问题

### Q1: 启动时报 `Address already in use`？
```bash
# Windows 查看占用 5000 端口的进程
netstat -ano | findstr 5000
# 杀掉对应 PID
taskkill /F /PID <PID>
```

### Q2: PySpark 报 `Python worker failed to connect back`？
这是 Windows 多 Worker 兼容性问题。在 `setting.py` 中将 `SPARK_HOST` 设为 `local[1]`（已默认配置）。

### Q3: 没有 Kafka 能运行吗？
**可以。** 系统设计了 fallback 机制：检测不到 Kafka 时自动使用内存管道，不影响核心功能。

### Q4: HDFS 在 Windows 上怎么跑的？
Windows 不支持原生 HDFS。我们实现了一个**HDFS 兼容接口层**，在 Windows 上用本地文件系统模拟，接口与真实 HDFS 一致。切换到 Linux 集群时只需改 `base_path` 为 `hdfs://namenode:8020/`。

### Q5: 前端图表不刷新？
- 检查浏览器控制台是否有 SocketIO 连接错误
- 确认后端控制台在持续输出数据生成日志
- 刷新页面重试（偶尔的 400 错误不影响数据流）

### Q6: 数据是假的，怎么对接真实数据？
只需修改 `data_generator.py`，替换为真实 API 调用或数据库 CDC。其他组件（Kafka、Spark、HDFS、可视化）处理的是标准 JSON，无需修改。

### Q7: Spark 控制台大量 WARN 日志？
这是 Spark 的正常行为，**不是错误**。主要是 Hadoop 本地库缺失警告，不影响功能。

---

## 📚 参考资料

- 原始参考项目：[steveliu13/Spark_Kafka_Dashboard](https://github.com/steveliu13/Spark_Kafka_Dashboard)
- 课程案例：[Spark+Kafka构建实时分析Dashboard](http://dblab.xmu.edu.cn/post/8274/)
- 原项目博客：[简书 - Spark+Kafka构建实时数据分析面板](https://www.jianshu.com/p/c3e33f03c0dc)
- Flask-SocketIO 参考：[博客园 - Flask+SocketIO+ECharts](https://www.cnblogs.com/hhh5460/p/7397006.html)

---

## 📄 License

MIT License — 仅供学习交流使用。

---

> **给组员的话**：clone 下来 → `pip install flask flask-socketio pyspark` → `python run.py --spark` → 打开浏览器就行了。有问题先看 `⚠️注意事项.md`，搞不定在群里问。
