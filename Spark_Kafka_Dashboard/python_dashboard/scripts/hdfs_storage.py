"""
HDFS / 分布式存储层
Windows环境下使用本地文件系统模拟HDFS接口
生产环境可切换到真实的HDFS (hdfs://namenode:8020/)
"""
import os
import json
import time
from datetime import datetime

# 模拟HDFS的存储根目录（生产环境改为 hdfs:// 地址）
HDFS_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "hdfs_storage")


class HDFSStorage:
    """HDFS兼容存储层（本地模式 / 可切换到真实HDFS）"""

    def __init__(self, base_path=None):
        self.base_path = base_path or HDFS_ROOT
        self._ensure_dirs()

    def _ensure_dirs(self):
        """创建目录结构：模拟HDFS的分层存储"""
        dirs = ["raw_data", "processed", "warehouse", "analytics", "checkpoint"]
        for d in dirs:
            os.makedirs(os.path.join(self.base_path, d), exist_ok=True)

    def write_raw(self, data, filename=None):
        """写入原始数据（模拟HDFS写入）"""
        if filename is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"raw_{ts}.json"
        path = os.path.join(self.base_path, "raw_data", filename)
        with open(path, 'w', encoding='utf-8') as f:
            if isinstance(data, (list, dict)):
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(data))
        return path

    def write_processed(self, data, filename=None):
        """写入处理后的数据（经过Spark处理的结果）"""
        if filename is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"processed_{ts}.json"
        path = os.path.join(self.base_path, "processed", filename)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path

    def write_analytics(self, metrics, filename=None):
        """写入分析结果"""
        if filename is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"analytics_{ts}.json"
        path = os.path.join(self.base_path, "analytics", filename)
        metrics["_timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)
        return path

    def read(self, path):
        """读取文件"""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def list_files(self, subdir="raw_data"):
        """列出目录文件"""
        d = os.path.join(self.base_path, subdir)
        if os.path.exists(d):
            return sorted(os.listdir(d))
        return []

    def get_storage_stats(self):
        """获取存储统计"""
        stats = {}
        for subdir in ["raw_data", "processed", "warehouse", "analytics"]:
            d = os.path.join(self.base_path, subdir)
            if os.path.exists(d):
                files = os.listdir(d)
                size = sum(os.path.getsize(os.path.join(d, f)) for f in files)
                stats[subdir] = {"files": len(files), "size_kb": round(size / 1024, 2)}
        return stats


# 全局单例
storage = HDFSStorage()
