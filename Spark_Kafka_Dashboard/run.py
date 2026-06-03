"""
一键启动脚本 — 大数据综合应用系统
用法：
  python run.py              # 默认本地模式
  python run.py --spark      # PySpark增强模式
  python run.py --kafka      # Kafka消息队列模式（需Kafka运行）
  python run.py --full       # 全组件模式（Kafka + PySpark + HDFS）
"""
import os
import sys
import subprocess
import webbrowser

# 确保在正确目录
os.chdir(os.path.dirname(os.path.abspath(__file__)))


def check_dependencies():
    """检查依赖"""
    print("=" * 60)
    print("  大数据综合应用系统 - 环境检查")
    print("=" * 60)

    deps = {
        "Flask": None, "flask_socketio": None,
        "pyspark": None, "kafka": None
    }

    for mod in deps:
        try:
            __import__(mod)
            deps[mod] = "✅"
        except ImportError:
            deps[mod] = "⚠️ 未安装"

    for mod, status in deps.items():
        print(f"  {mod:20s} {status}")

    # 检查HDFS存储目录
    storage_dir = os.path.join(os.path.dirname(__file__), "python_dashboard", "hdfs_storage")
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
        print(f"  {'HDFS存储目录':20s} ✅ 已创建")
    else:
        print(f"  {'HDFS存储目录':20s} ✅ 已存在")

    print("=" * 60)
    return deps


def main():
    deps = check_dependencies()

    # 解析参数
    mode = "local"
    if "--spark" in sys.argv or "--full" in sys.argv:
        mode = "spark"
    if "--kafka" in sys.argv or "--full" in sys.argv:
        mode = "kafka" if mode != "spark" else "full"

    print(f"\n  启动模式: {mode}")
    print(f"  访问地址: http://localhost:5000")
    print(f"  仪表盘:   http://localhost:5000/dashboard")
    print("  按 Ctrl+C 停止\n")

    # 自动打开浏览器
    try:
        webbrowser.open("http://localhost:5000/dashboard")
    except:
        pass

    # 启动应用
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "python_dashboard"))
    os.chdir(os.path.join(os.path.dirname(__file__), "python_dashboard"))

    extra_args = []
    if mode in ("spark", "full"):
        extra_args.append("--spark")
    if mode in ("kafka", "full"):
        extra_args.append("--kafka")

    # 直接导入并运行app_full
    import app_full
    app_full.socketio.run(
        app_full.app,
        debug=False,
        host="0.0.0.0",
        port=5000,
        allow_unsafe_werkzeug=True
    )


if __name__ == '__main__':
    main()
